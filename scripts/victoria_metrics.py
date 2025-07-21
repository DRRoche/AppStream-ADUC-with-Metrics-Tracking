import requests
import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import urllib3

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class VictoriaMetricsClient:
    """
    Client for sending metrics to VictoriaMetrics with authentication support
    """
    
    def __init__(
        self, 
        vm_url: str, 
        job_name: str,
        username: str = None,
        password: str = None,
        verify_ssl: bool = False,
        timeout: int = 30,
        max_retries: int = 3,
        enable_compression: bool = True,
        batch_size: int = 1000
    ):
        self.vm_url = vm_url.rstrip('/')
        self.job_name = job_name
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.max_retries = max_retries
        self.enable_compression = enable_compression
        self.batch_size = batch_size
        
        # Setup session with connection pooling and authentication
        self.session = requests.Session()
        self.session.verify = verify_ssl
        
        # Add basic authentication if credentials provided
        if self.username and self.password:
            self.session.auth = (self.username, self.password)
            print(f"✅ VictoriaMetrics HTTPS authentication configured for user: {self.username}")
        else:
            print("ℹ️ No VictoriaMetrics authentication configured")
        
        # Setup connection adapter
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=0
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Metrics buffer for batching
        self.metrics_buffer = []
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
    def add_metric(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None, timestamp: Optional[int] = None):
        """Add a metric to the buffer"""
        if labels is None:
            labels = {}
        
        # Add job_name to labels automatically
        labels['job_name'] = self.job_name
        
        # Use current timestamp if not provided
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        
        metric = {
            'name': metric_name,
            'value': float(value),
            'labels': labels,
            'timestamp': timestamp
        }
        
        self.metrics_buffer.append(metric)
        self.logger.info(f"Added metric: {metric_name} = {value}, labels: {labels}")
        
        # Auto-flush if buffer is full
        if len(self.metrics_buffer) >= self.batch_size:
            self.flush_metrics()
    
    def flush_metrics(self) -> bool:
        """Send all buffered metrics to VictoriaMetrics"""
        if not self.metrics_buffer:
            self.logger.info("No metrics to flush")
            return True
        
        try:
            # Convert metrics to Prometheus format
            prometheus_data = self._convert_to_prometheus_format(self.metrics_buffer)
            
            self.logger.info(f"Flushing {len(self.metrics_buffer)} metrics to VictoriaMetrics")
            self.logger.info(f"Prometheus data sample: {prometheus_data[:500]}...")
            
            # Send to VictoriaMetrics
            success = self._send_to_victoriametrics(prometheus_data)
            
            if success:
                self.logger.info(f"✅ Successfully sent {len(self.metrics_buffer)} metrics to VictoriaMetrics")
                self.metrics_buffer.clear()
            else:
                self.logger.error(f"❌ Failed to send {len(self.metrics_buffer)} metrics to VictoriaMetrics")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Error flushing metrics: {str(e)}")
            return False
    
    def _convert_to_prometheus_format(self, metrics: List[Dict]) -> str:
        """Convert metrics to Prometheus exposition format"""
        lines = []
        
        for metric in metrics:
            # Build label string
            label_parts = []
            for key, value in metric['labels'].items():
                # Escape quotes in label values
                escaped_value = str(value).replace('"', '\\"')
                label_parts.append(f'{key}="{escaped_value}"')
            
            label_string = '{' + ','.join(label_parts) + '}' if label_parts else ''
            
            # Format: metric_name{labels} value timestamp
            line = f"{metric['name']}{label_string} {metric['value']} {metric['timestamp']}"
            lines.append(line)
        
        return '\n'.join(lines) + '\n'
    
    def _send_to_victoriametrics(self, prometheus_data: str) -> bool:
        """Send Prometheus formatted data to VictoriaMetrics"""
        
        url = f"{self.vm_url}/api/v1/import/prometheus"
        headers = {
            'Content-Type': 'text/plain; charset=utf-8'
        }
        
        if self.enable_compression:
            headers['Content-Encoding'] = 'gzip'
            import gzip
            data = gzip.compress(prometheus_data.encode('utf-8'))
        else:
            data = prometheus_data.encode('utf-8')
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Sending to VictoriaMetrics (attempt {attempt}): {url}")
                
                response = self.session.post(
                    url,
                    data=data,
                    headers=headers,
                    timeout=self.timeout
                )
                
                self.logger.info(f"VictoriaMetrics response: Status {response.status_code}")
                
                if response.status_code in [200, 204]:
                    self.logger.info(f"✅ Successfully sent metrics to VictoriaMetrics (attempt {attempt})")
                    return True
                else:
                    self.logger.warning(f"Unexpected status code {response.status_code}: {response.text}")
                    
            except requests.exceptions.Timeout:
                self.logger.warning(f"⏱️ Request timed out on attempt {attempt}")
            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"🔌 Connection error on attempt {attempt}: {str(e)}")
            except Exception as e:
                self.logger.error(f"❌ Request failed on attempt {attempt}: {str(e)}")
            
            if attempt < self.max_retries:
                wait_time = 2 ** attempt
                self.logger.info(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
        
        self.logger.error(f"❌ Failed to send metrics after {self.max_retries} attempts")
        return False
    
    def __enter__(self):
        """Context manager entry"""
        self.logger.info(f"Initializing VictoriaMetrics client for {self.vm_url}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - flush remaining metrics"""
        if self.metrics_buffer:
            self.logger.info("Flushing remaining metrics on exit...")
            self.flush_metrics()


class GlueJobMetrics:
    """Helper class for tracking Glue job metrics and stages"""
    
    def __init__(self, vm_client: VictoriaMetricsClient, job_name: str):
        self.vm_client = vm_client
        self.job_name = job_name
        self.job_start_time = time.time()
        self.current_stage = None
        self.stage_start_time = None
        
    def start_stage(self, stage_name: str):
        """Start tracking a processing stage"""
        if self.current_stage:
            self.end_stage(self.current_stage)
        
        self.current_stage = stage_name
        self.stage_start_time = time.time()
        print(f"🔄 Starting stage: {stage_name}")
    
    def end_stage(self, stage_name: str):
        """End tracking a processing stage"""
        if self.stage_start_time:
            duration = time.time() - self.stage_start_time
            self.vm_client.add_metric(
                'glue_stage_duration_seconds',
                duration,
                labels={'stage': stage_name}
            )
            print(f"✅ Completed stage: {stage_name} in {duration:.2f}s")
        
        self.current_stage = None
        self.stage_start_time = None
    
    def record_data_quality(self, df, dataset_name: str):
        """Record data quality metrics"""
        try:
            total_rows = df.count()
            
            # Calculate null percentages for each column
            null_counts = {}
            for column in df.columns:
                null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
                null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
                null_counts[column] = null_percentage
                
                self.vm_client.add_metric(
                    'glue_column_null_percentage',
                    null_percentage,
                    labels={'dataset': dataset_name, 'column': column}
                )
            
            # Overall data quality score (100% - average null percentage)
            avg_null_percentage = sum(null_counts.values()) / len(null_counts) if null_counts else 0
            quality_score = 100 - avg_null_percentage
            
            self.vm_client.add_metric(
                'glue_data_quality_score',
                quality_score,
                labels={'dataset': dataset_name}
            )
            
            return {'quality_score': quality_score, 'null_counts': null_counts}
            
        except Exception as e:
            print(f"Error calculating data quality: {str(e)}")
            return {'quality_score': 0, 'null_counts': {}}
    
    def record_job_completion(self, status: str, error_message: str = None):
        """Record job completion metrics"""
        duration = time.time() - self.job_start_time
        
        self.vm_client.add_metric(
            'glue_job_duration_seconds',
            duration,
            labels={'status': status}
        )
        
        self.vm_client.add_metric(
            'glue_job_completed_total',
            1,
            labels={'status': status}
        )
        
        if error_message:
            self.vm_client.add_metric(
                'glue_job_errors_total',
                1,
                labels={'error_type': 'processing_error'}
            )