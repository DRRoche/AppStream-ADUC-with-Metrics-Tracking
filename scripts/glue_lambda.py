import boto3
import json
import os
from datetime import datetime
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Trigger Glue job when new AppStream reports arrive (sessions or applications)
    """
    
    glue_client = boto3.client('glue')
    
    # Configuration from environment variables
    GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'glue-appstream-victoriametrics-job')
    VM_URL = os.environ.get('VM_URL', 'http://your-victoriametrics:8428')
    PROCESSED_DATA_BUCKET = os.environ.get('PROCESSED_DATA_BUCKET', 's3://[AppStream Usage log bucket]/analytics/')
    
    logger.info(f"Lambda triggered with event: {json.dumps(event, default=str)}")
    
    try:
        # Process each S3 event record
        for record in event['Records']:
            # Get S3 event details
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            event_name = record['eventName']
            
            logger.info(f"Processing S3 event: {event_name} for s3://{bucket}/{key}")
            
            # Only process new file creation events
            if not event_name.startswith('ObjectCreated'):
                logger.info(f"Skipping event {event_name} - not a file creation")
                continue
            
            # Determine report type based on folder
            report_type = ""
            if key.startswith('sessions/'):
                report_type = "sessions"
                logger.info("Detected: Session usage report")
            elif key.startswith('applications/'):
                report_type = "applications"
                logger.info("Detected: Application usage report")
            else:
                logger.info(f"Skipping file {key} - not in sessions/ or applications/ folder")
                continue
            
            # Extract date from filename if possible
            report_date = ""
            try:
                import re
                # Look for date pattern YYYY-MM-DD in filename
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', key)
                if date_match:
                    report_date = date_match.group(1)
                    logger.info(f"Extracted date from filename: {report_date}")
                else:
                    # Use current date if can't extract from filename
                    report_date = datetime.now().strftime('%Y-%m-%d')
                    logger.info(f"Using current date: {report_date}")
            except Exception as e:
                report_date = datetime.now().strftime('%Y-%m-%d')
                logger.warning(f"Error extracting date, using current: {report_date}, Error: {str(e)}")
            
            # Determine the folder path for processing
            folder_path = '/'.join(key.split('/')[:-1])  # Remove filename, keep folder path
            s3_path = f"s3://{bucket}/{folder_path}/"
            
            logger.info(f"Report type: {report_type}")
            logger.info(f"Will process S3 path: {s3_path}")
            logger.info(f"Report date: {report_date}")
            logger.info(f"Output location: {PROCESSED_DATA_BUCKET}")
            
            # Start the Glue job with all necessary parameters
            job_arguments = {
                '--VM_URL': VM_URL,
                '--APPSTREAM_REPORTS_S3_PATH': s3_path,
                '--PROCESSED_DATA_S3_PATH': PROCESSED_DATA_BUCKET,
                '--REPORT_DATE': report_date,
                '--REPORT_TYPE': report_type,  # sessions or applications
                '--TRIGGER_FILE': f"s3://{bucket}/{key}"
            }
            
            logger.info(f"Starting Glue job {GLUE_JOB_NAME} with arguments:")
            for key_arg, value_arg in job_arguments.items():
                logger.info(f"  {key_arg}: {value_arg}")
            
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments=job_arguments
            )
            
            job_run_id = response['JobRunId']
            logger.info(f"✅ Started Glue job successfully!")
            logger.info(f"Job Run ID: {job_run_id}")
            logger.info(f"Report Type: {report_type}")
            logger.info(f"Triggered by: s3://{bucket}/{key}")
            
            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Glue job started successfully',
                    'jobRunId': job_run_id,
                    'reportType': report_type,
                    'triggerFile': f"s3://{bucket}/{key}",
                    'reportDate': report_date,
                    'processPath': s3_path,
                    'outputPath': PROCESSED_DATA_BUCKET
                })
            }
    
    except Exception as e:
        logger.error(f"❌ Error starting Glue job: {str(e)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Failed to start Glue job'
            })
        }

# Test function for manual testing
def test_lambda_locally():
    """Test the lambda function locally"""
    
    # Sample S3 event for testing sessions
    test_event_sessions = {
        "Records": [
            {
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "appstream-logs-us-east-1-576195058639-pz2kta0e"},
                    "object": {"key": "sessions/2024/01/15/session-report-2024-01-15.csv"}
                }
            }
        ]
    }
    
    # Sample S3 event for testing applications
    test_event_applications = {
        "Records": [
            {
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "appstream-logs-us-east-1-576195058639-pz2kta0e"},
                    "object": {"key": "applications/2024/01/15/app-report-2024-01-15.csv"}
                }
            }
        ]
    }
    
    print("Testing sessions report:")
    result1 = lambda_handler(test_event_sessions, None)
    print(json.dumps(result1, indent=2))
    
    print("\nTesting applications report:")
    result2 = lambda_handler(test_event_applications, None)
    print(json.dumps(result2, indent=2))

if __name__ == "__main__":
    test_lambda_locally()