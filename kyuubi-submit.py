#!/usr/bin/env python3
"""
Kyuubi Batch Submit Tool

Submits PySpark/Spark jobs to Kyuubi REST API with support for:
- Local file upload via multipart form
- Remote resources (S3, HDFS, etc.) via JSON
- Job monitoring and log retrieval

Based on working curl test:
curl -k -u user --location --request POST \
  'https://kyuubi-server/api/v1/batches' \
  --form 'batchRequest={"batchType":"PYSPARK","resource":"placeholder","name":"Test","conf":{"spark.submit.deployMode":"cluster"}};type=application/json' \
  --form 'resourceFile=@script.py'
"""

import argparse
import requests
import json
import time
import getpass
import logging
import sys
import os
from urllib.parse import urljoin, urlparse
from datetime import datetime

# Disable SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Try to import requests_toolbelt for better multipart handling
try:
    from requests_toolbelt.multipart.encoder import MultipartEncoder
    HAS_TOOLBELT = True
except ImportError:
    HAS_TOOLBELT = False


def setup_logger(debug=False):
    """Setup logger configuration"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.WARNING)
    
    return logging.getLogger('kyuubi-submit')


class KyuubiBatchSubmitter:
    """Kyuubi REST API batch job submitter"""
    
    # Remote URI schemes that don't need uploading
    REMOTE_SCHEMES = {'hdfs', 's3', 's3a', 's3n', 'gs', 'wasb', 'wasbs', 
                      'abfs', 'abfss', 'http', 'https', 'ftp', 'local'}
    
    # Python file extensions -> PYSPARK batch type
    PYTHON_EXTENSIONS = {'.py', '.zip', '.egg'}
    
    def __init__(self, server, username, password, logger):
        self.server = server.rstrip('/')
        self.username = username
        self.password = password
        self.logger = logger
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = False
        
    def is_local_file(self, path):
        """Check if path is a local file (not a remote URI)"""
        if not path:
            return False
        parsed = urlparse(path)
        # If there's a scheme and it's a known remote scheme, it's not local
        if parsed.scheme and parsed.scheme.lower() in self.REMOTE_SCHEMES:
            return False
        # Otherwise treat as local file
        return True
    
    def expand_path(self, path):
        """Expand ~ and environment variables in path"""
        return os.path.expanduser(os.path.expandvars(path))
    
    def get_batch_type(self, resource):
        """Determine batch type based on resource file extension"""
        if not resource:
            return "SPARK"
        
        # Get extension from path or URI
        parsed = urlparse(resource)
        path = parsed.path if parsed.scheme else resource
        _, ext = os.path.splitext(path.lower())
        
        if ext in self.PYTHON_EXTENSIONS:
            return "PYSPARK"
        return "SPARK"
    
    def submit_batch(self, resource, classname=None, name=None, args=None, 
                     conf=None, pyfiles=None, jars=None):
        """
        Submit a batch job to Kyuubi.
        
        Args:
            resource: Main application file (local path or remote URI)
            classname: Main class (for JAR files only)
            name: Job name
            args: List of application arguments
            conf: Dict of Spark configuration
            pyfiles: Comma-separated list of Python files
            jars: Comma-separated list of JAR files
        
        Returns:
            dict: Batch response from Kyuubi
        """
        url = f"{self.server}/api/v1/batches"
        
        # Determine batch type
        batch_type = self.get_batch_type(resource)
        self.logger.info(f"Detected batch type: {batch_type}")
        
        # Build base configuration
        spark_conf = {"spark.submit.deployMode": "cluster"}
        if conf:
            spark_conf.update(conf)
        
        # Build batch request
        batch_request = {
            "batchType": batch_type,
            "name": name or "kyuubi-batch-job",
            "conf": spark_conf
        }
        
        # Add className only for SPARK (JAR) jobs
        if batch_type == "SPARK" and classname:
            batch_request["className"] = classname
        
        # Add args if provided
        if args:
            batch_request["args"] = args if isinstance(args, list) else args.split()
        
        # Add remote pyfiles/jars to conf
        if pyfiles:
            remote_pyfiles = [f for f in pyfiles.split(',') if not self.is_local_file(f.strip())]
            if remote_pyfiles:
                spark_conf["spark.submit.pyFiles"] = ",".join(remote_pyfiles)
        
        if jars:
            remote_jars = [j for j in jars.split(',') if not self.is_local_file(j.strip())]
            if remote_jars:
                spark_conf["spark.jars"] = ",".join(remote_jars)
        
        # Check if we need to upload the main resource
        resource_is_local = self.is_local_file(resource)
        
        if resource_is_local:
            # Use multipart upload
            return self._submit_with_upload(url, batch_request, resource)
        else:
            # Use JSON submission for remote resources
            batch_request["resource"] = resource
            return self._submit_json(url, batch_request)
    
    def _submit_json(self, url, batch_request):
        """Submit batch via JSON (for remote resources)"""
        self.logger.info("Using JSON submission (remote resource)")
        self.logger.debug(f"Batch request: {json.dumps(batch_request, indent=2)}")
        
        response = self.session.post(
            url,
            json=batch_request,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code not in (200, 201):
            self.logger.error(f"Submit failed: {response.status_code}")
            self.logger.error(response.text)
            raise Exception(f"Failed to submit batch: {response.status_code}")
        
        return response.json()
    
    def _submit_with_upload(self, url, batch_request, resource):
        """Submit batch with file upload via multipart form"""
        resource_path = self.expand_path(resource)
        
        if not os.path.exists(resource_path):
            raise FileNotFoundError(f"Resource file not found: {resource_path}")
        
        self.logger.info(f"Uploading resource: {resource_path}")
        
        # WORKAROUND: Kyuubi has a validation bug where it checks 
        # `require(resource != null)` BEFORE processing the uploaded file.
        # The server code flow is:
        #   1. Validate batchRequest (fails if resource is null)
        #   2. Process uploaded file
        #   3. Call batchRequest.setResource(tempFile.getPath) - overwrites our value
        # So we must provide a placeholder value to pass validation, even though
        # Kyuubi will overwrite it with the actual uploaded file path.
        batch_request["resource"] = "placeholder"
        
        self.logger.debug(f"Batch request: {json.dumps(batch_request, indent=2)}")
        
        if HAS_TOOLBELT:
            return self._submit_multipart_toolbelt(url, batch_request, resource_path)
        else:
            return self._submit_multipart_requests(url, batch_request, resource_path)
    
    def _submit_multipart_toolbelt(self, url, batch_request, resource_path):
        """Submit using requests_toolbelt MultipartEncoder"""
        self.logger.debug("Using requests_toolbelt for multipart encoding")
        
        filename = os.path.basename(resource_path)
        
        encoder = MultipartEncoder(
            fields={
                'batchRequest': (
                    '',
                    json.dumps(batch_request),
                    'application/json'
                ),
                'resourceFile': (
                    filename,
                    open(resource_path, 'rb'),
                    'application/octet-stream'
                )
            }
        )
        
        response = self.session.post(
            url,
            data=encoder,
            headers={'Content-Type': encoder.content_type}
        )
        
        if response.status_code not in (200, 201):
            self.logger.error(f"Submit failed: {response.status_code}")
            self.logger.error(response.text)
            raise Exception(f"Failed to submit batch: {response.status_code}")
        
        return response.json()
    
    def _submit_multipart_requests(self, url, batch_request, resource_path):
        """Submit using standard requests library multipart"""
        self.logger.debug("Using requests library for multipart encoding")
        
        filename = os.path.basename(resource_path)
        
        # Build multipart form data
        files = {
            'batchRequest': (
                None,
                json.dumps(batch_request),
                'application/json'
            ),
            'resourceFile': (
                filename,
                open(resource_path, 'rb'),
                'application/octet-stream'
            )
        }
        
        response = self.session.post(url, files=files)
        
        if response.status_code not in (200, 201):
            self.logger.error(f"Submit failed: {response.status_code}")
            self.logger.error(response.text)
            raise Exception(f"Failed to submit batch: {response.status_code}")
        
        return response.json()
    
    def get_batch_status(self, batch_id):
        """Get batch job status"""
        url = f"{self.server}/api/v1/batches/{batch_id}"
        response = self.session.get(url)
        
        if response.status_code != 200:
            raise Exception(f"Failed to get batch status: {response.status_code}")
        
        return response.json()
    
    def get_batch_log(self, batch_id, from_index=0, size=100):
        """Get batch job logs"""
        url = f"{self.server}/api/v1/batches/{batch_id}/localLog"
        params = {"from": from_index, "size": size}
        
        response = self.session.get(url, params=params)
        
        if response.status_code != 200:
            self.logger.warning(f"Failed to get logs: {response.status_code}")
            return {"logRowSet": [], "rowCount": 0}
        
        return response.json()
    
    def delete_batch(self, batch_id):
        """Delete/cancel a batch job"""
        url = f"{self.server}/api/v1/batches/{batch_id}"
        response = self.session.delete(url)
        return response.status_code in (200, 204)
    
    def wait_for_completion(self, batch_id, poll_interval=5, timeout=3600):
        """
        Wait for batch job to complete.
        
        Args:
            batch_id: Batch job ID
            poll_interval: Seconds between status checks
            timeout: Maximum seconds to wait
        
        Returns:
            dict: Final batch status
        """
        terminal_states = {'FINISHED', 'FAILED', 'KILLED', 'CANCELED', 'ERROR'}
        start_time = time.time()
        
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Batch {batch_id} did not complete within {timeout}s")
            
            status = self.get_batch_status(batch_id)
            state = status.get('state', 'UNKNOWN')
            app_state = status.get('appState', '')
            app_id = status.get('appId', '')
            
            self.logger.info(f"State: {state} | App State: {app_state} | App ID: {app_id}")
            
            if state in terminal_states:
                return status
            
            time.sleep(poll_interval)


def parse_conf_string(conf_string):
    """Parse comma-separated key=value pairs into dict"""
    conf_dict = {}
    if conf_string:
        for item in conf_string.split(','):
            item = item.strip()
            if '=' in item:
                key, value = item.split('=', 1)
                conf_dict[key.strip()] = value.strip()
    return conf_dict


def main():
    parser = argparse.ArgumentParser(
        description='Submit batch jobs to Kyuubi REST API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Submit local Python script
  %(prog)s --server https://kyuubi:10099 --username user --resource ./script.py

  # Submit with arguments
  %(prog)s --server https://kyuubi:10099 --username user --resource ./script.py \\
    --args "arg1 arg2 arg3" --name "My Job"

  # Submit JAR with main class
  %(prog)s --server https://kyuubi:10099 --username user \\
    --resource s3a://bucket/app.jar --classname com.example.Main

  # Submit with extra configuration
  %(prog)s --server https://kyuubi:10099 --username user --resource ./script.py \\
    --conf "spark.executor.memory=4g,spark.executor.cores=2"
        """
    )
    
    parser.add_argument('--server', required=True, help='Kyuubi server URL')
    parser.add_argument('--username', required=True, help='Username for authentication')
    parser.add_argument('--password', help='Password (will prompt if not provided)')
    parser.add_argument('--resource', required=True, help='Main application file (local path or remote URI)')
    parser.add_argument('--classname', help='Main class name (for JAR files)')
    parser.add_argument('--name', default='kyuubi-batch-job', help='Job name')
    parser.add_argument('--args', help='Application arguments (space-separated)')
    parser.add_argument('--conf', help='Spark config (comma-separated key=value pairs)')
    parser.add_argument('--pyfiles', help='Python files to include (comma-separated)')
    parser.add_argument('--jars', help='JAR files to include (comma-separated)')
    parser.add_argument('--wait', action='store_true', help='Wait for job completion')
    parser.add_argument('--timeout', type=int, default=3600, help='Wait timeout in seconds')
    parser.add_argument('--poll-interval', type=int, default=5, help='Poll interval in seconds')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logger(args.debug)
    
    # Get password
    password = args.password
    if not password:
        password = getpass.getpass(f"Password for {args.username}: ")
    
    # Parse configuration
    conf = parse_conf_string(args.conf) if args.conf else None
    
    try:
        # Create submitter
        submitter = KyuubiBatchSubmitter(
            server=args.server,
            username=args.username,
            password=password,
            logger=logger
        )
        
        # Submit batch
        logger.info(f"Submitting job: {args.name}")
        
        result = submitter.submit_batch(
            resource=args.resource,
            classname=args.classname,
            name=args.name,
            args=args.args,
            conf=conf,
            pyfiles=args.pyfiles,
            jars=args.jars
        )
        
        batch_id = result.get('id')
        logger.info(f"Batch submitted successfully! ID: {batch_id}")
        logger.info(f"State: {result.get('state')}")
        
        if args.debug:
            logger.debug(f"Full response: {json.dumps(result, indent=2)}")
        
        # Wait for completion if requested
        if args.wait:
            logger.info(f"Waiting for completion (timeout: {args.timeout}s)...")
            
            final_status = submitter.wait_for_completion(
                batch_id,
                poll_interval=args.poll_interval,
                timeout=args.timeout
            )
            
            final_state = final_status.get('state')
            app_state = final_status.get('appState')
            
            logger.info(f"Job completed - State: {final_state}, App State: {app_state}")
            
            # Get logs
            logs = submitter.get_batch_log(batch_id, size=50)
            if logs.get('logRowSet'):
                logger.info("=== Logs ===")
                for line in logs['logRowSet']:
                    print(line)
            
            if final_state == 'FINISHED' and app_state == 'FINISHED':
                logger.info("Job completed successfully!")
                sys.exit(0)
            else:
                logger.error(f"Job ended with state: {final_state}")
                sys.exit(1)
        else:
            # Print status check command
            print(f"\nTo check status:")
            print(f"  curl -k -u {args.username} '{args.server}/api/v1/batches/{batch_id}'")
            
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
