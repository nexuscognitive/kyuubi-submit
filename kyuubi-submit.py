#!/usr/bin/env python3

import argparse
import requests
import json
import time
import getpass
import logging
import sys
import yaml
import os
from urllib.parse import urljoin, urlparse
from datetime import datetime

# Disable SSL warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
def setup_logger(debug=False):
    """Setup logger configuration"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Reduce noise from requests library
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.WARNING)
    
    return logging.getLogger('kyuubi-submit')

class KyuubiBatchSubmitter:
    # Known remote URI schemes that don't need uploading
    REMOTE_SCHEMES = {'hdfs', 's3', 's3a', 's3n', 'gs', 'wasb', 'wasbs', 'abfs', 'abfss', 
                      'http', 'https', 'ftp', 'local'}
    
    # Python file extensions
    PYTHON_EXTENSIONS = {'.py', '.zip', '.egg'}
    
    def __init__(self, server, username, password, logger, history_server=None):
        self.server = server
        self.username = username
        self.password = password
        self.logger = logger
        self.history_server = history_server
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = False  # Disable SSL verification
        
    def is_local_file(self, path):
        """
        Determine if a path refers to a local file that needs uploading.
        Returns True for local files, False for remote URIs.
        """
        if not path:
            return False
        
        # Parse the path to check for URI scheme
        parsed = urlparse(path)
        
        # If it has a known remote scheme, it's not local
        if parsed.scheme and parsed.scheme.lower() in self.REMOTE_SCHEMES:
            return False
        
        # If it has a scheme we don't recognize, treat it as remote
        if parsed.scheme and len(parsed.scheme) > 1:  # len > 1 to exclude Windows drive letters
            self.logger.debug(f"Unknown scheme '{parsed.scheme}' for path '{path}', treating as remote")
            return False
        
        # Check if the file exists locally
        expanded_path = os.path.expanduser(os.path.expandvars(path))
        if os.path.isfile(expanded_path):
            return True
        
        # If file doesn't exist locally, assume it's a remote path without scheme
        self.logger.debug(f"Path '{path}' not found locally, treating as remote")
        return False
    
    def expand_path(self, path):
        """Expand user home and environment variables in path."""
        return os.path.expanduser(os.path.expandvars(path))
    
    def get_file_extension(self, path):
        """Get lowercase file extension from path or URI."""
        # Handle URIs by extracting the path component
        parsed = urlparse(path)
        if parsed.scheme:
            path = parsed.path
        return os.path.splitext(path)[1].lower()
    
    def is_python_resource(self, resource):
        """Determine if the resource is a Python file based on extension."""
        ext = self.get_file_extension(resource)
        return ext in self.PYTHON_EXTENSIONS
    
    def parse_conf(self, conf_string):
        """Parse comma-separated key=value pairs into a dictionary"""
        conf_dict = {}
        if conf_string:
            for item in conf_string.split(','):
                item = item.strip()
                if '=' in item:
                    key, value = item.split('=', 1)
                    conf_dict[key.strip()] = value.strip()
        return conf_dict
    
    def format_history_url(self, app_id):
        """Format Spark History Server URL"""
        if self.history_server and app_id:
            history_base = self.history_server.rstrip('/')
            if not history_base.startswith(('http://', 'https://')):
                history_base = f"http://{history_base}"
            if ':18080' not in history_base and not any(f":{p}" in history_base for p in range(1, 65536)):
                history_base = f"{history_base}:18080"
            return f"{history_base}/history/{app_id}/"
        return None
    
    def classify_paths(self, paths):
        """
        Classify a list of paths into local files and remote URIs.
        Returns (local_files, remote_uris) where local_files is list of (original_path, expanded_path)
        """
        if not paths:
            return [], []
        
        local_files = []
        remote_uris = []
        
        for path in paths:
            path = path.strip()
            if not path:
                continue
            if self.is_local_file(path):
                local_files.append((path, self.expand_path(path)))
            else:
                remote_uris.append(path)
        
        return local_files, remote_uris
        
    def submit_batch(self, resource, classname, name, args, conf, py_files, jars):
        """Submit a batch job to Kyuubi using multipart form data if local files present."""
        url = urljoin(self.server, "/api/v1/batches")
        
        # Determine batch type based on resource extension
        is_pyspark = self.is_python_resource(resource)
        batch_type = "pyspark" if is_pyspark else "spark"
        self.logger.info(f"Detected batch type: {batch_type}")
        
        # Determine if main resource is local
        resource_is_local = self.is_local_file(resource)
        resource_expanded = self.expand_path(resource) if resource_is_local else None
        
        # Classify pyFiles and jars
        py_files_list = []
        if py_files:
            if isinstance(py_files, str):
                py_files_list = [f.strip() for f in py_files.split(',') if f.strip()]
            else:
                py_files_list = [f for f in py_files if f]
        
        jars_list = []
        if jars:
            if isinstance(jars, str):
                jars_list = [j.strip() for j in jars.split(',') if j.strip()]
            else:
                jars_list = [j for j in jars if j]
        
        local_py_files, remote_py_files = self.classify_paths(py_files_list)
        local_jars, remote_jars = self.classify_paths(jars_list)
        
        # Check if we need multipart upload
        has_local_files = resource_is_local or local_py_files or local_jars
        
        # Build batch request object
        batch_request = {
            "batchType": batch_type,
            "name": name,
            "args": args.split(',') if isinstance(args, str) and args else args or []
        }
        
        # Only set resource in JSON if it's remote
        if not resource_is_local:
            batch_request["resource"] = resource
        
        # Set className only for JARs (not for PySpark)
        if classname and not is_pyspark:
            batch_request["className"] = classname

        # Handle conf
        if isinstance(conf, str):
            conf_dict = self.parse_conf(conf)
        else:
            conf_dict = conf or {}
        conf_dict["spark.submit.deployMode"] = "cluster"
        batch_request["conf"] = conf_dict
        
        # Add remote pyFiles to batch request
        if remote_py_files:
            batch_request["pyFiles"] = remote_py_files
        
        # Add remote jars to batch request
        if remote_jars:
            batch_request["jars"] = remote_jars
        
        self.logger.info(f"Submitting job: {name}")
        
        if has_local_files:
            self.logger.info("Detected local files - using multipart upload")
            response = self._submit_multipart(url, batch_request, resource_expanded, local_py_files, local_jars)
        else:
            self.logger.info("All resources are remote - using JSON submission")
            response = self._submit_json(url, batch_request)
        
        if response.status_code not in [200, 201]:
            self.logger.error(f"Error submitting job: {response.status_code}")
            self.logger.error(response.text)
            raise Exception(f"Failed to submit batch: {response.status_code}")
        
        batch = response.json()
        return batch['id']
    
    def _submit_json(self, url, batch_request):
        """Submit batch using JSON (no file uploads)."""
        self.logger.debug(f"Batch config: {json.dumps(batch_request, indent=2)}")
        return self.session.post(
            url,
            headers={"Content-Type": "application/json"},
            json=batch_request
        )
    
    def _submit_multipart(self, url, batch_request, resource_file, local_py_files, local_jars):
        """Submit batch using multipart form data with file uploads."""
        self.logger.debug(f"Batch request: {json.dumps(batch_request, indent=2)}")
        
        # Build multipart form data
        files = []
        opened_files = []  # Track opened files for cleanup
        
        try:
            # Add batch request as JSON part
            files.append(
                ('batchRequest', (None, json.dumps(batch_request), 'application/json'))
            )
            
            # Add main resource file if local
            if resource_file:
                self.logger.info(f"Uploading main resource: {resource_file}")
                f = open(resource_file, 'rb')
                opened_files.append(f)
                files.append(
                    ('resourceFile', (os.path.basename(resource_file), f, 'application/octet-stream'))
                )
            
            # Add extra resource files (pyFiles and jars)
            for original_path, expanded_path in local_py_files:
                self.logger.info(f"Uploading pyFile: {expanded_path}")
                f = open(expanded_path, 'rb')
                opened_files.append(f)
                files.append(
                    ('extraResourceFiles', (os.path.basename(expanded_path), f, 'application/octet-stream'))
                )
            
            for original_path, expanded_path in local_jars:
                self.logger.info(f"Uploading jar: {expanded_path}")
                f = open(expanded_path, 'rb')
                opened_files.append(f)
                files.append(
                    ('extraResourceFiles', (os.path.basename(expanded_path), f, 'application/octet-stream'))
                )
            
            # Send multipart request
            response = self.session.post(url, files=files)
            return response
            
        finally:
            # Clean up opened files
            for f in opened_files:
                f.close()
    
    def get_batch_status(self, batch_id):
        """Get batch status"""
        url = urljoin(self.server, f"/api/v1/batches/{batch_id}")
        response = self.session.get(url)
        if response.status_code != 200:
            self.logger.error(f"Error getting status: {response.status_code}")
            return None
        return response.json()
    
    def get_batch_logs(self, batch_id, from_index=0, size=1000):
        """Get batch logs"""
        url = urljoin(self.server, f"/api/v1/batches/{batch_id}/localLog")
        params = {"from": from_index, "size": size}
        response = self.session.get(url, params=params)
        if response.status_code != 200:
            self.logger.warning(f"Error getting logs: {response.status_code}")
            return None
        return response.json()
    
    def print_all_logs(self, batch_id):
        """Retrieve and print all logs"""
        self.logger.info("Retrieving job logs...")
        print("\n" + "="*60 + " JOB OUTPUT " + "="*60)
        
        from_index = 0
        size = 1000
        total_printed = 0
        
        while True:
            log_data = self.get_batch_logs(batch_id, from_index, size)
            if not log_data:
                self.logger.warning("No log data available")
                break
            log_rows = log_data.get('logRowSet', [])
            for row in log_rows:
                print(row)
                total_printed += 1
            total = log_data.get('total', 0)
            if from_index + len(log_rows) >= total:
                break
            from_index += len(log_rows)
        
        print("="*132 + "\n")
        self.logger.info(f"Retrieved {total_printed} log lines")
    
    def monitor_job(self, batch_id, show_logs=True):
        """Monitor job until completion"""
        self.logger.info(f"Monitoring batch {batch_id}")
        
        start_time = datetime.now()
        previous_state = None
        previous_app_state = None
        spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        spinner_idx = 0
        
        while True:
            try:
                status = self.get_batch_status(batch_id)
                
                if status:
                    state = status.get('state', 'UNKNOWN')
                    app_state = status.get('appState', '')
                    app_id = status.get('appId', '')
                    app_url = status.get('appUrl', '')
                    elapsed = (datetime.now() - start_time).seconds
                    
                    if state not in ['FINISHED', 'ERROR', 'CANCELLED']:
                        sys.stdout.write(f'\r{spinner[spinner_idx % len(spinner)]} State: {state} | App State: {app_state or "N/A"} (elapsed: {elapsed}s)')
                        sys.stdout.flush()
                        spinner_idx += 1
                    
                    if state != previous_state or app_state != previous_app_state:
                        sys.stdout.write('\r' + ' '*80 + '\r')
                        self.logger.info(f"Batch state: {state} | App state: {app_state or 'N/A'}")
                        previous_state = state
                        previous_app_state = app_state
                        
                        if app_id:
                            self.logger.info(f"Spark App ID: {app_id}")
                            history_url = self.format_history_url(app_id)
                            if history_url:
                                self.logger.info(f"Spark History URL: {history_url}")
                            elif app_url:
                                self.logger.info(f"Spark UI URL: {app_url}")
                    
                    if state == 'FINISHED':
                        sys.stdout.write('\r' + ' '*80 + '\r')
                        elapsed_min = elapsed // 60
                        elapsed_sec = elapsed % 60
                        self.logger.info(f"Job completed in {elapsed_min}m {elapsed_sec}s")
                        if app_state:
                            self.logger.info(f"Final application state: {app_state}")
                        app_diagnostic = status.get('appDiagnostic', '')
                        if app_diagnostic:
                            self.logger.info(f"Application diagnostics: {app_diagnostic}")
                        if show_logs:
                            self.print_all_logs(batch_id)
                        else:
                            self.logger.info("Logs available but not displayed (use --show-logs to see them)")
                        return app_state if app_state else state
                    
                    elif state in ['ERROR', 'CANCELLED']:
                        sys.stdout.write('\r' + ' '*80 + '\r')
                        self.logger.error(f"Batch terminated with state: {state}")
                        app_diagnostic = status.get('appDiagnostic', '')
                        if app_diagnostic:
                            self.logger.error(f"Application diagnostics: {app_diagnostic}")
                        if show_logs:
                            self.print_all_logs(batch_id)
                        return state
                
            except Exception as e:
                self.logger.error(f"Error monitoring job: {e}")
            
            time.sleep(10)


def load_yaml_config(config_file):
    """Load configuration from YAML file"""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def merge_configs(yaml_config, cli_args):
    """Merge YAML config with CLI arguments. CLI arguments take precedence."""
    merged = {}
    if yaml_config:
        merged = yaml_config.copy()
    cli_dict = vars(cli_args)
    for key, value in cli_dict.items():
        if value is not None and key not in ['config_file', 'func']:
            if isinstance(value, bool):
                if value or key not in merged:
                    merged[key] = value
            else:
                merged[key] = value
    return merged


def get_password(config, username):
    """Get password from config, environment variable, or prompt"""
    password = config.get('password')
    if not password:
        password = os.environ.get('KYUUBI_SUBMIT_PASSWORD')
        if password:
            logging.getLogger('kyuubi-submit').debug("Using password from KYUUBI_SUBMIT_PASSWORD environment variable")
    if not password:
        password = getpass.getpass(f"Password for {username}: ")
    return password


def normalize_queue(queue_name):
    if not queue_name.startswith("root."):
        return f"root.default.{queue_name}"
    return queue_name


def inject_yunikorn_spark_configs(config):
    """Sets Spark configs for YuniKorn queue labels."""
    if 'sparkConf' not in config:
        config['sparkConf'] = {}
    queue = config.get("queue")
    if queue:
        queue = normalize_queue(queue)
        logging.getLogger('kyuubi-submit').debug(f"Queue name after normalizing: {queue}")
        config['sparkConf']["spark.kubernetes.driver.label.queue"] = queue
        config['sparkConf']["spark.kubernetes.executor.label.queue"] = queue


def main():
    parser = argparse.ArgumentParser(
        description='Submit and monitor Kyuubi batch job',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--config-file', help='YAML configuration file')
    parser.add_argument('--server', help='Kyuubi server URL')
    parser.add_argument('--history-server', help='Spark History Server URL')
    parser.add_argument('--username', help='Username')
    parser.add_argument('--password', help='Password (will check KYUUBI_SUBMIT_PASSWORD env var, then prompt if not provided)')
    parser.add_argument('--resource', help='JAR or Python file path (local files will be auto-uploaded)')
    parser.add_argument('--classname', help='Main class name (for JAR files only, ignored for PySpark)')
    parser.add_argument('--name', help='Job name')
    parser.add_argument('--queue', help='Optional YuniKorn queue name to submit the job into')
    parser.add_argument('--args', help='Comma-separated arguments or list in YAML')
    parser.add_argument('--conf', help='Comma-separated Spark configs or dict in YAML')
    parser.add_argument('--pyfiles', help='Comma-separated Python files (local files will be auto-uploaded)')
    parser.add_argument('--jars', help='Comma-separated JAR files (local files will be auto-uploaded)')
    parser.add_argument('--show-logs', action='store_true', help='Display job logs after completion')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    yaml_config = None
    if args.config_file:
        try:
            yaml_config = load_yaml_config(args.config_file)
        except Exception as e:
            print(f"Error loading config file: {e}")
            sys.exit(1)
    
    config = merge_configs(yaml_config, args)

    required_fields = ['server', 'username', 'resource', 'name']
    missing_fields = [field for field in required_fields if field not in config or not config[field]]
    if missing_fields:
        print(f"Error: Missing required fields: {', '.join(missing_fields)}")
        sys.exit(1)

    inject_yunikorn_spark_configs(config)
    logger = setup_logger(config.get('debug', False))
    
    if args.config_file:
        logger.info(f"Loaded configuration from: {args.config_file}")
    
    password = get_password(config, config['username'])
    
    submitter = KyuubiBatchSubmitter(
        config['server'], 
        config['username'], 
        password, 
        logger,
        config.get('history_server')
    )
    
    try:
        batch_id = submitter.submit_batch(
            config['resource'],
            config.get('classname'),
            config['name'],
            config.get('args', ''),
            config.get('conf', ''),
            config.get('pyfiles'),
            config.get('jars')
        )
        logger.info(f"Batch submitted successfully! ID: {batch_id}")
        
        final_state = submitter.monitor_job(batch_id, show_logs=config.get('show_logs', False))
        
        if final_state in ['FAILED', 'ERROR']:
            logger.error("Job failed!")
            sys.exit(1)
        elif final_state == 'CANCELLED':
            logger.warning("Job was cancelled")
            sys.exit(2)
        elif final_state in ['SUCCESS', 'SUCCEEDED', 'FINISHED']:
            logger.info("Job completed successfully!")
        else:
            logger.warning(f"Job ended with unexpected state: {final_state}")
            sys.exit(3)
            
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
