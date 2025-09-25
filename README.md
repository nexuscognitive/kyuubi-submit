# Kyuubi Batch Submit

A powerful Python CLI tool for submitting and monitoring Apache Kyuubi batch jobs with support for Spark and PySpark applications.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [Command Line Options](#command-line-options)
  - [YAML Configuration](#yaml-configuration)
  - [Environment Variables](#environment-variables)
- [Usage Examples](#usage-examples)
  - [Basic Examples](#basic-examples)
  - [Advanced Examples](#advanced-examples)
- [Password Management](#password-management)
- [Spark History Server Integration](#spark-history-server-integration)
- [Output and Logging](#output-and-logging)
- [Contributing](#contributing)
- [License](#license)

## Features

- ✅ **Submit Spark/PySpark batch jobs** to Kyuubi server via REST API
- ✅ **Real-time job monitoring** with status updates and progress spinner
- ✅ **Automatic log retrieval** upon job completion (optional)
- ✅ **YAML configuration support** for reusable job configurations
- ✅ **Flexible authentication** with password from CLI, YAML, environment variable, or interactive prompt
- ✅ **Spark History Server integration** with formatted URLs
- ✅ **SSL support** with certificate verification options
- ✅ **Comprehensive error handling** and diagnostic information
- ✅ **Support for additional JARs and Python files**
- ✅ **Configurable Spark properties** via CLI or YAML
- ✅ **Exit codes** for scripting and automation

## Installation

### Prerequisites

- Python 3.6+
- Pip package manager

### Install Dependencies

```bash
# Clone the repository
git clone https://github.com/yourusername/kyuubi-batch-submit.git
cd kyuubi-batch-submit

# Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

**requirements.txt:**
```txt
requests>=2.28.0
pyyaml>=5.4.1
urllib3>=1.26.0
```

### Make Script Executable

```bash
chmod +x kyuubi_submit.py
```

## Quick Start

### Basic Spark Job Submission

```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username your-username \
  --resource s3a://bucket/spark-examples.jar \
  --classname org.apache.spark.examples.SparkPi \
  --name "My-First-Job" \
  --args "1000"
```

### Using YAML Configuration

Create a configuration file `job-config.yaml`:
```yaml
server: https://kyuubi.example.com
username: your-username
resource: s3a://bucket/spark-examples.jar
classname: org.apache.spark.examples.SparkPi
name: SparkPi-Job
args: "1000"
show_logs: true
```

Run with:
```bash
python kyuubi_submit.py --config-file job-config.yaml
```

## Configuration

### Command Line Options

| Option | Required | Description | Example |
|--------|----------|-------------|---------|
| `--server` | Yes | Kyuubi server URL | `https://kyuubi.example.com` |
| `--username` | Yes | Authentication username | `john.doe@company.com` |
| `--password` | No | Authentication password (see [Password Management](#password-management)) | `--password mypass` |
| `--resource` | Yes | Path to JAR or Python file | `s3a://bucket/app.jar` |
| `--classname` | No* | Main class name (*required for JAR files) | `com.example.MainClass` |
| `--name` | Yes | Job name | `"Production ETL Job"` |
| `--args` | No | Space-separated job arguments | `"input.csv output.parquet"` |
| `--conf` | No | Comma-separated Spark configurations | `"spark.executor.memory=4g,spark.executor.instances=10"` |
| `--pyfiles` | No | Comma-separated Python dependencies | `"utils.py,libs.zip"` |
| `--jars` | No | Comma-separated additional JARs | `"mysql-connector.jar,custom-udfs.jar"` |
| `--history-server` | No | Spark History Server URL | `spark-history.example.com` |
| `--show-logs` | No | Display job logs after completion | Flag only |
| `--debug` | No | Enable debug logging | Flag only |
| `--config-file` | No | YAML configuration file path | `config/production.yaml` |

### YAML Configuration

YAML files can contain all configuration options. CLI arguments override YAML values.

#### Basic Structure
```yaml
# Connection settings
server: https://kyuubi.example.com
username: user@company.com
password: optional-password  # Can be omitted for security

# Job settings
resource: s3a://bucket/path/to/app.jar
classname: com.example.MainApp  # Optional for PySpark
name: My-Batch-Job

# Optional settings
args: "arg1 arg2"  # Can be string or list
show_logs: true
debug: false
history_server: spark-history.example.com

# Spark configuration (as dictionary)
conf:
  spark.executor.memory: 8g
  spark.executor.instances: 20
  spark.dynamicAllocation.enabled: true

# Dependencies (as lists)
pyfiles:
  - s3a://bucket/utils.py
  - s3a://bucket/libraries.zip

jars:
  - s3a://bucket/lib/mysql-connector.jar
  - s3a://bucket/lib/custom-udfs.jar
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `KYUUBI_SUBMIT_PASSWORD` | Default password for authentication |

## Usage Examples

### Basic Examples

#### 1. Simple Spark Job
```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username user@company.com \
  --resource s3a://bucket/spark-examples.jar \
  --classname org.apache.spark.examples.SparkPi \
  --name "Calculate-Pi" \
  --args "1000"
```

#### 2. PySpark Job (No Class Required)
```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username user@company.com \
  --resource s3a://bucket/etl_job.py \
  --name "Python-ETL" \
  --args "--input data/raw --output data/processed"
```

#### 3. Using Environment Variable for Password
```bash
export KYUUBI_SUBMIT_PASSWORD='my-secure-password'

python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username user@company.com \
  --resource s3a://bucket/app.jar \
  --classname com.example.App \
  --name "Secure-Job"
```

### Advanced Examples

#### 1. Complex Spark Job with Dependencies
```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username user@company.com \
  --resource s3a://bucket/main-app.jar \
  --classname com.company.analytics.MainJob \
  --name "Analytics-Pipeline" \
  --args "--date 2024-01-01 --mode production" \
  --jars "s3a://bucket/lib/postgres-driver.jar,s3a://bucket/lib/custom-udfs.jar" \
  --conf "spark.executor.memory=16g,spark.executor.instances=50,spark.sql.adaptive.enabled=true" \
  --history-server spark-history.company.com \
  --show-logs
```

#### 2. PySpark with Multiple Files
```yaml
# pyspark-ml-job.yaml
server: https://kyuubi.example.com
username: data-scientist@company.com
resource: s3a://ml-bucket/train_model.py
name: ML-Model-Training

args:
  - --model-type
  - random-forest
  - --input
  - s3a://ml-bucket/training-data/
  - --output
  - s3a://ml-bucket/models/

pyfiles:
  - s3a://ml-bucket/lib/feature_engineering.py
  - s3a://ml-bucket/lib/model_utils.py
  - s3a://ml-bucket/lib/dependencies.zip

conf:
  spark.executor.memory: 32g
  spark.executor.cores: 8
  spark.executor.instances: 20
  spark.python.worker.memory: 16g
  spark.sql.execution.arrow.pyspark.enabled: true

show_logs: true
history_server: http://spark-history.company.com:18080
```

Run with:
```bash
python kyuubi_submit.py --config-file pyspark-ml-job.yaml
```

#### 3. Override YAML Configuration
```bash
# Override executor memory and show logs
python kyuubi_submit.py \
  --config-file base-config.yaml \
  --conf "spark.executor.memory=32g" \
  --show-logs
```

## Password Management

The tool supports multiple methods for password authentication, in order of precedence:

1. **Command Line Argument** (highest priority)
   ```bash
   python kyuubi_submit.py --password 'my-password' ...
   ```

2. **YAML Configuration File**
   ```yaml
   password: my-password
   ```

3. **Environment Variable**
   ```bash
   export KYUUBI_SUBMIT_PASSWORD='my-password'
   python kyuubi_submit.py ...
   ```

4. **Interactive Prompt** (most secure)
   ```
   Password for user@company.com: [hidden input]
   ```

## Spark History Server Integration

When configured, the tool automatically formats and displays Spark History Server URLs:

```bash
# Configure via CLI
python kyuubi_submit.py \
  --history-server spark-history.example.com \
  ...

# Or in YAML
history_server: spark-history.example.com
```

Supported formats:
- `spark-history.example.com` → `http://spark-history.example.com:18080`
- `spark-history.example.com:18080` → `http://spark-history.example.com:18080`
- `https://spark-history.example.com:8443` → `https://spark-history.example.com:8443`

## Output and Logging

### Standard Output

```
2024-01-15 10:30:45 - kyuubi-submit - INFO - Submitting job: Analytics-Pipeline
2024-01-15 10:30:46 - kyuubi-submit - INFO - Batch submitted successfully! ID: batch_2024_01_15_abc123
2024-01-15 10:30:46 - kyuubi-submit - INFO - Monitoring batch batch_2024_01_15_abc123
2024-01-15 10:30:56 - kyuubi-submit - INFO - Batch state: PENDING | App state: N/A
2024-01-15 10:31:06 - kyuubi-submit - INFO - Batch state: RUNNING | App state: RUNNING
2024-01-15 10:31:06 - kyuubi-submit - INFO - Spark App ID: application_1234567890_0001
2024-01-15 10:31:06 - kyuubi-submit - INFO - Spark History URL: http://spark-history.example.com:18080/history/application_1234567890_0001/
⠼ State: RUNNING | App State: RUNNING (elapsed: 45s)
2024-01-15 10:35:30 - kyuubi-submit - INFO - Batch state: FINISHED | App state: SUCCESS
2024-01-15 10:35:30 - kyuubi-submit - INFO - Job completed in 4m 44s
2024-01-15 10:35:30 - kyuubi-submit - INFO - Final application state: SUCCESS
2024-01-15 10:35:30 - kyuubi-submit - INFO - Job completed successfully!
```

### Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Job failed |
| 2 | Job cancelled |
| 3 | Unexpected state |

### Debug Mode

Enable verbose logging with `--debug`:
```bash
python kyuubi_submit.py --debug --config-file job.yaml
```

## Troubleshooting

### Common Issues

1. **SSL Certificate Errors**
   - The tool disables SSL verification by default
   - For production use, consider implementing proper certificate handling

2. **Authentication Failures**
   - Verify username and password
   - Check if Kyuubi server requires specific authentication method

3. **Resource Not Found**
   - Ensure S3/HDFS paths are accessible
   - Check IAM roles and permissions

4. **Class Not Found**
   - Verify the main class name is correct
   - Ensure all required JARs are included

### Getting Help

1. Enable debug mode for detailed logging
2. Check Kyuubi server logs
3. Verify network connectivity to Kyuubi server

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Apache Kyuubi team for the excellent big data SQL gateway
- Apache Spark community
- Contributors and users of this tool

---

**Note**: Replace example URLs and credentials with your actual values. Never commit sensitive information like passwords to version control.
