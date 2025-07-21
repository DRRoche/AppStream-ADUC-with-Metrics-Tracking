import sys
import time
import logging
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, current_timestamp, when, sum as spark_sum, count as spark_count, 
    avg, max as spark_max, min as spark_min, regexp_extract, 
    to_timestamp, date_format, hour, dayofweek, month, year,
    split, explode, trim, upper, lower, desc, asc, isnan, isnull
)

# Import your custom modules
from victoria_metrics import VictoriaMetricsClient, GlueJobMetrics

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Process AppStream usage reports and send metrics to VictoriaMetrics"""
    
    # Arguments specific to AppStream usage reports - Updated with REPORT_TYPE
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'VM_URL',
        'APPSTREAM_REPORTS_S3_PATH',    # S3 path to specific report folder (sessions/ or applications/)
        'PROCESSED_DATA_S3_PATH',       # Where to save processed data (analytics/ folder)
        'REPORT_DATE',                  # Date extracted from filename or current date
        'REPORT_TYPE'                   # 'sessions' or 'applications'
    ])
    
    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Get report type for specialized processing
    report_type = args['REPORT_TYPE']
    logger.info(f"Processing {report_type} reports from: {args['APPSTREAM_REPORTS_S3_PATH']}")
    
    # Initialize VictoriaMetrics client
    with VictoriaMetricsClient(
        vm_url=args['VM_URL'],
        job_name=args['JOB_NAME'],
        batch_size=1000  # AppStream reports can be large
    ) as vm_client:
        
        # Initialize metrics collector
        metrics = GlueJobMetrics(vm_client, args['JOB_NAME'])
        
        try:
            logger.info(f"Starting AppStream {report_type} report processing: {args['JOB_NAME']}")
            logger.info(f"Report Date: {args['REPORT_DATE']}")
            logger.info(f"Input Path: {args['APPSTREAM_REPORTS_S3_PATH']}")
            logger.info(f"Output Path: {args['PROCESSED_DATA_S3_PATH']}")
            
            # Stage 1: Data Extraction - Read AppStream Usage Reports
            metrics.start_stage('extraction')
            
            # AppStream usage reports are typically CSV files
            try:
                appstream_dynamic_frame = glueContext.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={
                        "paths": [args['APPSTREAM_REPORTS_S3_PATH']],
                        "recurse": True  # Process files in subdirectories
                    },
                    format="csv",
                    format_options={
                        "withHeader": True,
                        "separator": ",",
                        "quoteChar": '"',
                        "escapeChar": '\\',
                        "ignoreLeadingWhiteSpace": True,
                        "ignoreTrailingWhiteSpace": True
                    }
                )
                
                raw_df = appstream_dynamic_frame.toDF()
                logger.info(f"Extracted {raw_df.count()} AppStream {report_type} records from S3")
                
            except Exception as e:
                logger.error(f"Failed to read {report_type} reports: {str(e)}")
                # Try without header in case format is different
                logger.info("Attempting to read without header...")
                appstream_dynamic_frame = glueContext.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={
                        "paths": [args['APPSTREAM_REPORTS_S3_PATH']],
                        "recurse": True
                    },
                    format="csv",
                    format_options={
                        "withHeader": False,
                        "separator": ","
                    }
                )
                raw_df = appstream_dynamic_frame.toDF()
                logger.info(f"Extracted {raw_df.count()} records without header")
            
            # Log the schema for debugging
            logger.info(f"AppStream {report_type} report schema:")
            raw_df.printSchema()
            
            # Show sample data for debugging
            logger.info(f"Sample {report_type} data:")
            raw_df.show(5, truncate=False)
            
            metrics.end_stage('extraction')
            
            # Stage 2: Data Quality Analysis
            quality_metrics = metrics.record_data_quality(raw_df, f'appstream_{report_type}_reports')
            logger.info(f"Data quality score: {quality_metrics['quality_score']:.2f}%")
            
            # Stage 3: Data Transformation and Analysis
            metrics.start_stage('transformation')
            
            # Get column names (they might vary between sessions and applications reports)
            columns = raw_df.columns
            logger.info(f"Available columns in {report_type} report: {columns}")
            
            # Clean and standardize AppStream data based on report type
            if report_type == 'sessions':
                cleaned_df = process_session_reports(raw_df, logger)
            elif report_type == 'applications':
                cleaned_df = process_application_reports(raw_df, logger)
            else:
                logger.warning(f"Unknown report type: {report_type}, using generic processing")
                cleaned_df = process_generic_reports(raw_df, logger)
            
            # Add processing metadata
            final_df = cleaned_df.withColumn('ProcessedAt', current_timestamp()) \
                               .withColumn('ReportType', lit(report_type)) \
                               .withColumn('ProcessingDate', lit(args['REPORT_DATE']))
            
            logger.info(f"After transformation: {final_df.count()} clean AppStream {report_type} records")
            
            metrics.end_stage('transformation')
            
            # Stage 4: Generate Analytics and Metrics
            metrics.start_stage('analytics')
            
            # Generate metrics based on report type
            if report_type == 'sessions':
                generate_session_metrics(final_df, vm_client, logger)
            elif report_type == 'applications':
                generate_application_metrics(final_df, vm_client, logger)
            
            # Common metrics for both types
            generate_common_metrics(final_df, vm_client, report_type, logger)
            
            metrics.end_stage('analytics')
            
            # Stage 5: Data Loading - Save processed data
            metrics.start_stage('loading')
            
            # Convert back to DynamicFrame for output
            output_dynamic_frame = glueContext.create_dynamic_frame.from_df(
                final_df, 
                glueContext, 
                f"processed_appstream_{report_type}"
            )
            
            # Save processed data partitioned by report type and date
            partition_keys = ['ReportType']
            if 'ProcessingDate' in final_df.columns:
                partition_keys.append('ProcessingDate')
            
            output_path = f"{args['PROCESSED_DATA_S3_PATH']}{report_type}/"
            
            glueContext.write_dynamic_frame.from_options(
                frame=output_dynamic_frame,
                connection_type="s3",
                connection_options={
                    "path": output_path,
                    "partitionKeys": partition_keys
                },
                format="parquet",
                format_options={
                    "compression": "snappy"
                }
            )
            
            logger.info(f"Processed AppStream {report_type} data saved to: {output_path}")
            
            # Save a summary report
            total_records = final_df.count()
            summary_data = [(
                report_type,
                total_records,
                args['REPORT_DATE'],
                args['APPSTREAM_REPORTS_S3_PATH'],
                output_path,
                datetime.now()
            )]
            
            summary_df = spark.createDataFrame(
                summary_data,
                ['ReportType', 'TotalRecords', 'ReportDate', 'InputPath', 'OutputPath', 'ProcessedAt']
            )
            
            summary_dynamic_frame = glueContext.create_dynamic_frame.from_df(
                summary_df, glueContext, f"appstream_{report_type}_summary"
            )
            
            glueContext.write_dynamic_frame.from_options(
                frame=summary_dynamic_frame,
                connection_type="s3",
                connection_options={
                    "path": f"{args['PROCESSED_DATA_S3_PATH']}summary/"
                },
                format="json"
            )
            
            metrics.end_stage('loading')
            
            # Record final metrics
            vm_client.add_metric(
                'glue_records_processed_total',
                total_records,
                labels={'data_type': f'appstream_{report_type}', 'report_date': args['REPORT_DATE']}
            )
            
            # Record successful completion
            metrics.record_job_completion('success')
            
            logger.info("="*60)
            logger.info(f"APPSTREAM {report_type.upper()} REPORT PROCESSING COMPLETE")
            logger.info("="*60)
            logger.info(f"Report Type: {report_type}")
            logger.info(f"Report Date: {args['REPORT_DATE']}")
            logger.info(f"Records Processed: {total_records:,}")
            logger.info(f"Input Path: {args['APPSTREAM_REPORTS_S3_PATH']}")
            logger.info(f"Output Path: {output_path}")
            logger.info(f"Metrics sent to: {args['VM_URL']}")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"❌ AppStream {report_type} processing failed: {str(e)}")
            vm_client.add_metric(
                'glue_job_errors_total',
                1,
                labels={'error_type': type(e).__name__, 'report_type': report_type}
            )
            metrics.record_job_completion('failed', str(e))
            raise
        
        finally:
            # Ensure all metrics are sent to VictoriaMetrics
            vm_client.flush_metrics()
            job.commit()


def process_session_reports(df, logger):
    """Process AppStream session reports specifically"""
    from pyspark.sql.functions import lit
    
    logger.info("Processing session-specific transformations")
    
    try:
        # Common session report columns (adjust based on your actual schema)
        cleaned_df = df
        
        # Handle common session columns if they exist
        if 'UserId' in df.columns:
            cleaned_df = cleaned_df.filter(col('UserId').isNotNull()) \
                                 .withColumn('UserId', trim(col('UserId')))
        
        if 'SessionId' in df.columns:
            cleaned_df = cleaned_df.filter(col('SessionId').isNotNull())
        
        # Handle timestamps if they exist
        timestamp_columns = ['SessionStartTime', 'SessionEndTime', 'ConnectTime', 'DisconnectTime']
        for ts_col in timestamp_columns:
            if ts_col in df.columns:
                cleaned_df = cleaned_df.withColumn(
                    ts_col,
                    to_timestamp(col(ts_col))
                )
        
        # Calculate session duration if possible
        if 'SessionStartTime' in df.columns and 'SessionEndTime' in df.columns:
            cleaned_df = cleaned_df.withColumn(
                'SessionDurationMinutes',
                (col('SessionEndTime').cast('long') - col('SessionStartTime').cast('long')) / 60
            )
        
        return cleaned_df
        
    except Exception as e:
        logger.error(f"Error processing session reports: {str(e)}")
        return df


def process_application_reports(df, logger):
    """Process AppStream application reports specifically"""
    from pyspark.sql.functions import lit
    
    logger.info("Processing application-specific transformations")
    
    try:
        cleaned_df = df
        
        # Handle common application columns if they exist
        if 'ApplicationName' in df.columns:
            cleaned_df = cleaned_df.filter(col('ApplicationName').isNotNull()) \
                                 .withColumn('ApplicationName', trim(col('ApplicationName')))
        
        if 'UserId' in df.columns:
            cleaned_df = cleaned_df.filter(col('UserId').isNotNull()) \
                                 .withColumn('UserId', trim(col('UserId')))
        
        # Handle usage duration if it exists
        duration_columns = ['UsageDuration', 'ApplicationUsageTime', 'Duration']
        for dur_col in duration_columns:
            if dur_col in df.columns:
                cleaned_df = cleaned_df.withColumn(
                    'UsageDurationMinutes',
                    col(dur_col).cast('double')
                )
                break
        
        return cleaned_df
        
    except Exception as e:
        logger.error(f"Error processing application reports: {str(e)}")
        return df


def process_generic_reports(df, logger):
    """Generic processing for unknown report types"""
    
    logger.info("Processing generic AppStream report")
    
    try:
        # Basic cleaning that should work for most reports
        cleaned_df = df
        
        # Remove completely empty rows
        cleaned_df = cleaned_df.dropna(how='all')
        
        # Trim string columns
        for column in df.columns:
            if dict(df.dtypes)[column] == 'string':
                cleaned_df = cleaned_df.withColumn(column, trim(col(column)))
        
        return cleaned_df
        
    except Exception as e:
        logger.error(f"Error in generic processing: {str(e)}")
        return df


def generate_session_metrics(df, vm_client, logger):
    """Generate metrics specific to session reports"""
    
    logger.info("Generating session-specific metrics")
    
    try:
        # Basic session metrics
        total_sessions = df.count()
        vm_client.add_metric('appstream_sessions_total', total_sessions, 
                           labels={'report_type': 'sessions'})
        
        if 'UserId' in df.columns:
            unique_users = df.select('UserId').distinct().count()
            vm_client.add_metric('appstream_unique_users', unique_users,
                               labels={'report_type': 'sessions'})
        
        # Duration metrics if available
        if 'SessionDurationMinutes' in df.columns:
            duration_stats = df.select('SessionDurationMinutes').summary()
            duration_stats.show()
            
            avg_duration = df.agg(avg(col('SessionDurationMinutes'))).collect()[0][0] or 0
            max_duration = df.agg(spark_max(col('SessionDurationMinutes'))).collect()[0][0] or 0
            total_duration = df.agg(spark_sum(col('SessionDurationMinutes'))).collect()[0][0] or 0
            
            vm_client.add_metric('appstream_avg_session_duration_minutes', avg_duration)
            vm_client.add_metric('appstream_max_session_duration_minutes', max_duration)
            vm_client.add_metric('appstream_total_usage_minutes', total_duration)
            
        logger.info(f"Generated metrics for {total_sessions} sessions")
        
    except Exception as e:
        logger.error(f"Error generating session metrics: {str(e)}")


def generate_application_metrics(df, vm_client, logger):
    """Generate metrics specific to application reports"""
    
    logger.info("Generating application-specific metrics")
    
    try:
        total_records = df.count()
        vm_client.add_metric('appstream_application_records_total', total_records,
                           labels={'report_type': 'applications'})
        
        if 'ApplicationName' in df.columns:
            unique_apps = df.select('ApplicationName').distinct().count()
            vm_client.add_metric('appstream_unique_applications', unique_apps,
                               labels={'report_type': 'applications'})
            
            # Top applications
            top_apps = df.groupBy('ApplicationName').count() \
                        .orderBy(desc('count')).limit(10).collect()
            
            for i, row in enumerate(top_apps):
                vm_client.add_metric(
                    'appstream_top_applications_usage',
                    row['count'],
                    labels={
                        'application': row['ApplicationName'][:50],
                        'rank': str(i + 1),
                        'report_type': 'applications'
                    }
                )
        
        logger.info(f"Generated metrics for {total_records} application records")
        
    except Exception as e:
        logger.error(f"Error generating application metrics: {str(e)}")


def generate_common_metrics(df, vm_client, report_type, logger):
    """Generate common metrics for any report type"""
    
    try:
        total_records = df.count()
        
        # Basic count metrics
        vm_client.add_metric('appstream_total_records', total_records,
                           labels={'report_type': report_type})
        
        # Data freshness metric
        vm_client.add_metric('appstream_last_processed_timestamp', 
                           int(time.time()),
                           labels={'report_type': report_type})
        
        logger.info(f"Generated common metrics for {total_records} {report_type} records")
        
    except Exception as e:
        logger.error(f"Error generating common metrics: {str(e)}")


if __name__ == "__main__":
    main()