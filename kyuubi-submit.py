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
from urllib.parse import urljoin
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
    def __init__(self, server, username, password, logger, history_server=None):
        self.server = server
        self.username = username
        self.password = password
        self.logger = logger
        self.history_server = history_server
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = False  # Disable SSL verification
        
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
            # Remove any trailing slashes from history server
            history_base = self.history_server.rstrip('/')
            # Add protocol if not present
            if not history_base.startswith(('http://', 'https://')):
                history_base = f"http://{history_base}"
            # Add default port if not specified
            if ':18080' not in history_base and not any(f":{p}" in history_base for p in range(1, 65536)):
                history_base = f"{history_base}:18080"
            return f"{history_base}/history/{app_id}/"
        return None
        
    def submit_batch(self, resource, classname, name, args, conf, py_files, jars):
        """Submit a batch job to Kyuubi"""
        url = urljoin(self.server, "/api/v1/batches")
        
        # Build batch configuration
        batch_config = {
            "batchType": "SPARK",
            "resource": resource,
            "name": name,
            "args": args.split(',') if isinstance(args, str) and args else args or []
        }
        
        # Use default className for PySpark if not provided
        if classname:
            batch_config["className"] = classname
        else:
            batch_config["className"] = "org.apache.spark.deploy.PythonRunner"

        # Handle conf - could be string (from CLI) or dict (from YAML)
        if isinstance(conf, str):
            conf_dict = self.parse_conf(conf)
        else:
            conf_dict = conf or {}
            
        # Always set cluster deploy mode
        conf_dict["spark.submit.deployMode"] = "cluster"
        batch_config["conf"] = conf_dict
        
        # Add pyFiles if provided
        if py_files:
            if isinstance(py_files, str):
                batch_config["pyFiles"] = [f.strip() for f in py_files.split(',')]
            elif isinstance(py_files, list):
                batch_config["pyFiles"] = py_files
        
        # Add jars if provided
        if jars:
            if isinstance(jars, str):
                batch_config["jars"] = [j.strip() for j in jars.split(',')]
            elif isinstance(jars, list):
                batch_config["jars"] = jars
        
        self.logger.info(f"Submitting job: {name}")
        self.logger.debug(f"Batch config: {json.dumps(batch_config, indent=2)}")
        
        response = self.session.post(
            url,
            headers={"Content-Type": "application/json"},
            json=batch_config
        )
        
        if response.status_code != 200:
            self.logger.error(f"Error submitting job: {response.status_code}")
            self.logger.error(response.text)
            raise Exception(f"Failed to submit batch: {response.status_code}")
        
        batch = response.json()
        return batch['id']
    
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
                    
                    # Show spinner for non-terminal states
                    if state not in ['FINISHED', 'ERROR', 'CANCELLED']:
                        sys.stdout.write(f'\r{spinner[spinner_idx % len(spinner)]} State: {state} | App State: {app_state or "N/A"} (elapsed: {elapsed}s)')
                        sys.stdout.flush()
                        spinner_idx += 1
                    
                    # Log state changes
                    if state != previous_state or app_state != previous_app_state:
                        sys.stdout.write('\r' + ' '*80 + '\r')  # Clear spinner line
                        
                        self.logger.info(f"Batch state: {state} | App state: {app_state or 'N/A'}")
                        previous_state = state
                        previous_app_state = app_state
                        
                        if app_id:
                            self.logger.info(f"Spark App ID: {app_id}")
                            
                            # Display history server URL if configured
                            history_url = self.format_history_url(app_id)
                            if history_url:
                                self.logger.info(f"Spark History URL: {history_url}")
                            elif app_url:
                                self.logger.info(f"Spark UI URL: {app_url}")
                    
                    # Check if batch has finished
                    if state == 'FINISHED':
                        sys.stdout.write('\r' + ' '*80 + '\r')  # Clear spinner line
                        
                        elapsed_min = elapsed // 60
                        elapsed_sec = elapsed % 60
                        self.logger.info(f"Job completed in {elapsed_min}m {elapsed_sec}s")
                        
                        # Check app state for actual success/failure
                        if app_state:
                            self.logger.info(f"Final application state: {app_state}")
                        
                        # Print diagnostic info if available
                        app_diagnostic = status.get('appDiagnostic', '')
                        if app_diagnostic:
                            self.logger.info(f"Application diagnostics: {app_diagnostic}")
                        
                        # Print logs if requested
                        if show_logs:
                            self.print_all_logs(batch_id)
                        else:
                            self.logger.info("Logs available but not displayed (use --show-logs to see them)")
                            self.logger.info(f"To view logs later: kyuubi-ctl log batch {batch_id}")
                        
                        # Return the app state (SUCCESS/FAILED) if available, otherwise the batch state
                        return app_state if app_state else state
                    
                    # Handle error or cancelled states
                    elif state in ['ERROR', 'CANCELLED']:
                        sys.stdout.write('\r' + ' '*80 + '\r')  # Clear spinner line
                        
                        self.logger.error(f"Batch terminated with state: {state}")
                        
                        # Print diagnostic info if available
                        app_diagnostic = status.get('appDiagnostic', '')
                        if app_diagnostic:
                            self.logger.error(f"Application diagnostics: {app_diagnostic}")
                        
                        # Try to get logs even in error state if requested
                        if show_logs:
                            self.print_all_logs(batch_id)
                        else:
                            self.logger.info("Error logs available but not displayed (use --show-logs to see them)")
                            self.logger.info(f"To view logs: kyuubi-ctl log batch {batch_id}")
                        
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
    
    # Start with YAML config if provided
    if yaml_config:
        merged = yaml_config.copy()
    
    # Override with CLI arguments (only if they are explicitly provided)
    cli_dict = vars(cli_args)
    for key, value in cli_dict.items():
        # Skip None values and special keys
        if value is not None and key not in ['config_file', 'func']:
            # For boolean flags, only override if True (explicitly set)
            if isinstance(value, bool):
                if value or key not in merged:
                    merged[key] = value
            else:
                merged[key] = value
    
    return merged

def get_password(config, username):
    """Get password from config, environment variable, or prompt"""
    # First check if password is in config
    password = config.get('password')
    
    # If not, check environment variable
    if not password:
        password = os.environ.get('KYUUBI_SUBMIT_PASSWORD')
        if password:
            logging.getLogger('kyuubi-submit').debug("Using password from KYUUBI_SUBMIT_PASSWORD environment variable")
    
    # If still not found, prompt
    if not password:
        password = getpass.getpass(f"Password for {username}: ")
    
    return password

def inject_yunikorn_spark_configs(config):
    """Sets Spark configs for YuniKorn queue labels and user.info annotations."""
    if 'sparkConf' not in config:
        config['sparkConf'] = {}

    queue = config.get("queue")
    if queue:
        logging.getLogger('kyuubi-submit').debug(f"Queue name: {queue}")
        # Set YuniKorn queue labels for driver and executor
        config['sparkConf']["spark.kubernetes.driver.label.queue"] = queue
        config['sparkConf']["spark.kubernetes.executor.label.queue"] = queue

def main():
    parser = argparse.ArgumentParser(
        description='Submit and monitor Kyuubi batch job',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--config-file', help='YAML configuration file')
    parser.add_argument('--server', help='Kyuubi server URL')
    parser.add_argument('--history-server', help='Spark History Server URL (e.g., spark-history.example.com or http://spark-history.example.com:18080)')
    parser.add_argument('--username', help='Username')
    parser.add_argument('--password', help='Password (will check KYUUBI_SUBMIT_PASSWORD env var, then prompt if not provided)')
    parser.add_argument('--resource', help='JAR or Python file path')
    parser.add_argument('--classname', help='Main class name (optional for PySpark)')
    parser.add_argument('--name', help='Job name')
    parser.add_argument('--queue', help='Optional YuniKorn queue name to submit the job into')
    parser.add_argument('--args', help='Space-separated arguments or list in YAML')
    parser.add_argument('--conf', help='Comma-separated Spark configs or dict in YAML')
    parser.add_argument('--pyfiles', help='Comma-separated Python files or list in YAML')
    parser.add_argument('--jars', help='Comma-separated JAR files or list in YAML')
    parser.add_argument('--show-logs', action='store_true', help='Display job logs after completion')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Load YAML config if provided
    yaml_config = None
    if args.config_file:
        try:
            yaml_config = load_yaml_config(args.config_file)
        except Exception as e:
            print(f"Error loading config file: {e}")
            sys.exit(1)
    
    # Merge configurations
    config = merge_configs(yaml_config, args)

    # Validate required fields
    required_fields = ['server', 'username', 'resource', 'name']
    missing_fields = [field for field in required_fields if field not in config or not config[field]]
    if missing_fields:
        print(f"Error: Missing required fields: {', '.join(missing_fields)}")
        print("These must be provided either in the config file or as command-line arguments")
        sys.exit(1)

    # Inject YuniKorn spark configs
    inject_yunikorn_spark_configs(config)

    # Setup logger
    logger = setup_logger(config.get('debug', False))
    
    if args.config_file:
        logger.info(f"Loaded configuration from: {args.config_file}")
    
    # Get password (from config, env var, or prompt)
    password = get_password(config, config['username'])
    
    # Create submitter with history server if provided
    submitter = KyuubiBatchSubmitter(
        config['server'], 
        config['username'], 
        password, 
        logger,
        config.get('history_server')
    )
    
    try:
        # Submit job
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
        
        # Monitor job
        final_state = submitter.monitor_job(batch_id, show_logs=config.get('show_logs', False))
        
        # Exit with appropriate code based on app state
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
