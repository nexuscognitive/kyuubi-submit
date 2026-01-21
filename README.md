# Kyuubi Batch Submit

A Python CLI tool for submitting and monitoring Apache Kyuubi batch jobs with support for **local file uploads**, Spark, and PySpark applications.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Local File Upload](#local-file-upload)
- [Configuration](#configuration)
  - [Command Line Options](#command-line-options)
  - [YAML Configuration](#yaml-configuration)
  - [Environment Variables](#environment-variables)
- [Usage Examples](#usage-examples)
- [Password Management](#password-management)
- [Spark History Server Integration](#spark-history-server-integration)
- [Output and Logging](#output-and-logging)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

- ✅ **Local file upload support** - automatically uploads local JARs, Python files, and resources
- ✅ **Smart path detection** - distinguishes local files from remote URIs (S3, HDFS, etc.)
- ✅ **Mixed local/remote resources** - combine local uploads with remote URIs in the same job
- ✅ **Submit Spark/PySpark batch jobs** to Kyuubi server via REST API
- ✅ **Real-time job monitoring** with status updates and progress spinner
- ✅ **Automatic log retrieval** upon job completion
- ✅ **YAML configuration support** for reusable job configurations
- ✅ **Flexible authentication** with password from CLI, YAML, environment variable, or interactive prompt
- ✅ **Spark History Server integration** with formatted URLs
- ✅ **YuniKorn queue support** for Kubernetes deployments
- ✅ **Exit codes** for scripting and automation

## Installation

### Prerequisites

- Python 3.6+
- Kyuubi server with REST API enabled
- Kyuubi server with resource upload enabled (`kyuubi.batch.resource.upload.enabled=true`)

### Install Dependencies

```bash
pip install requests pyyaml urllib3
```

Or using requirements.txt:

```bash
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

### Submit a Local JAR

```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username your-username \
  --resource ./my-spark-app.jar \
  --classname com.example.MainClass \
  --name "My-Job"
```

### Submit a Local Python Script

```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username your-username \
  --resource ./my_pyspark_job.py \
  --name "My-PySpark-Job"
```

### Submit with Remote Resources (S3/HDFS)

```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username your-username \
  --resource s3a://bucket/spark-app.jar \
  --classname com.example.MainClass \
  --name "Remote-Job"
```

## Local File Upload

The tool automatically detects and uploads local files to the Kyuubi server. This works for:

- **Main resource** (`--resource`) - JAR or Python file
- **Additional JARs** (`--jars`)
- **Python files** (`--pyfiles`)
- **Data/config files** (`--files`)

### How It Works

1. The tool checks if each path is a local file or a remote URI
2. Local files are uploaded via Kyuubi's multipart form API
3. Remote URIs (S3, HDFS, etc.) are passed directly to Spark
4. You can mix local and remote resources in the same submission

### Supported Remote Schemes

The following URI schemes are recognized as remote (not uploaded):
- `hdfs://`, `s3://`, `s3a://`, `s3n://`
- `gs://` (Google Cloud Storage)
- `wasb://`, `wasbs://`, `abfs://`, `abfss://` (Azure)
- `http://`, `https://`, `ftp://`

### Example: Mixed Local and Remote Resources

```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username your-username \
  --resource ./my-app.jar \
  --classname com.example.MainClass \
  --name "Mixed-Resources-Job" \
  --jars "./local-lib.jar,s3a://bucket/remote-lib.jar" \
  --files "./config.json,s3a://bucket/data.csv"
```

In this example:
- `my-app.jar` → uploaded to Kyuubi
- `local-lib.jar` → uploaded to Kyuubi
- `s3a://bucket/remote-lib.jar` → passed to Spark as-is
- `config.json` → uploaded to Kyuubi
- `s3a://bucket/data.csv` → passed to Spark as-is

### Important Notes for `--files`

> ⚠️ **Kyuubi versions before 1.10.x with the filename fix**: Uploaded files are renamed with a timestamp suffix (e.g., `config.json` becomes `config-20260121144955-1.json`). This breaks `SparkFiles.get("config.json")`.
> 
> **Workarounds:**
> 1. Upload files to S3/HDFS first and use remote URIs
> 2. Use a patched Kyuubi server that preserves original filenames
> 3. In your Spark code, search for files by prefix pattern

## Configuration

### Command Line Options

| Option | Required | Description |
|--------|----------|-------------|
| `--server` | Yes | Kyuubi server URL |
| `--username` | Yes | Authentication username |
| `--password` | No | Password (or use env var / prompt) |
| `--resource` | Yes | JAR or Python file (local or remote) |
| `--classname` | No* | Main class (*required for JARs) |
| `--name` | Yes | Job name |
| `--args` | No | Job arguments (space-separated) |
| `--conf` | No | Spark configs (comma-separated `key=value`) |
| `--jars` | No | Additional JARs (comma-separated, local or remote) |
| `--pyfiles` | No | Python dependencies (comma-separated, local or remote) |
| `--files` | No | Files to distribute (comma-separated, local or remote) |
| `--queue` | No | YuniKorn queue name |
| `--history-server` | No | Spark History Server URL |
| `--show-logs` | No | Display logs after completion |
| `--debug` | No | Enable debug logging |
| `--config-file` | No | YAML configuration file |

### YAML Configuration

Create a `job-config.yaml`:

```yaml
server: https://kyuubi.example.com
username: your-username
# password: optional (will prompt if not set)

resource: ./my-app.jar
classname: com.example.MainClass
name: My-Batch-Job

args: "--input data.csv --output results/"

jars:
  - ./lib/mysql-connector.jar
  - s3a://bucket/common-utils.jar

pyfiles:
  - ./utils.py

files:
  - s3a://bucket/config.json  # Remote recommended for files

conf:
  spark.executor.memory: 4g
  spark.executor.instances: 10
  spark.dynamicAllocation.enabled: "true"

queue: my-team-queue
history_server: spark-history.example.com
show_logs: true
debug: false
```

Run with:

```bash
python kyuubi_submit.py --config-file job-config.yaml
```

Override YAML values with CLI:

```bash
python kyuubi_submit.py --config-file job-config.yaml --conf "spark.executor.memory=8g"
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `KYUUBI_SUBMIT_PASSWORD` | Default password for authentication |

## Usage Examples

### PySpark with Dependencies

```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username data-scientist \
  --resource ./train_model.py \
  --name "ML-Training" \
  --pyfiles "./utils.py,./preprocessing.py" \
  --files "s3a://bucket/model-config.json" \
  --conf "spark.executor.memory=16g,spark.executor.instances=20" \
  --show-logs
```

### Spark Job with Custom JARs

```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username engineer \
  --resource ./etl-job.jar \
  --classname com.company.ETLJob \
  --name "Daily-ETL" \
  --jars "./jdbc-driver.jar,s3a://libs/avro-tools.jar" \
  --args "--date 2024-01-15 --env production" \
  --queue etl-queue
```

### Using YuniKorn Queue

```bash
python kyuubi_submit.py \
  --server https://kyuubi.example.com \
  --username user \
  --resource s3a://bucket/app.jar \
  --classname com.example.App \
  --name "Queued-Job" \
  --queue analytics-team
```

The queue name is automatically normalized (e.g., `analytics-team` → `root.default.analytics-team`) and applied as Kubernetes labels for YuniKorn scheduling.

## Password Management

Password resolution order (first match wins):

1. **CLI argument**: `--password 'mypass'`
2. **YAML config**: `password: mypass`
3. **Environment variable**: `export KYUUBI_SUBMIT_PASSWORD='mypass'`
4. **Interactive prompt**: Secure input (recommended)

## Spark History Server Integration

Configure the history server to get direct links to your job:

```bash
python kyuubi_submit.py \
  --history-server spark-history.example.com \
  ...
```

The tool automatically formats URLs and displays them when the job starts:

```
INFO - Spark App ID: application_1234567890_0001
INFO - Spark History URL: http://spark-history.example.com:18080/history/application_1234567890_0001/
```

## Output and Logging

### Standard Output

```
2024-01-15 10:30:45 - kyuubi-submit - INFO - Detected batch type: SPARK
2024-01-15 10:30:45 - kyuubi-submit - INFO - Submitting job: My-ETL-Job
2024-01-15 10:30:45 - kyuubi-submit - INFO - Detected local files - using multipart upload
2024-01-15 10:30:45 - kyuubi-submit - INFO - Extra resources map: {'spark.jars': 'mysql-connector.jar'}
2024-01-15 10:30:45 - kyuubi-submit - INFO - Uploading main resource: ./etl-job.jar
2024-01-15 10:30:45 - kyuubi-submit - INFO - Uploading jar: ./mysql-connector.jar
2024-01-15 10:30:46 - kyuubi-submit - INFO - Batch submitted successfully! ID: abc123-def456
2024-01-15 10:30:46 - kyuubi-submit - INFO - Monitoring batch abc123-def456
⠼ State: RUNNING | App State: RUNNING (elapsed: 45s)
2024-01-15 10:35:30 - kyuubi-submit - INFO - Job completed in 4m 44s
2024-01-15 10:35:30 - kyuubi-submit - INFO - Final application state: SUCCEEDED
```

### Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Job failed / Error |
| 2 | Job cancelled |
| 3 | Unexpected state |

### Debug Mode

Enable verbose logging:

```bash
python kyuubi_submit.py --debug ...
```

This shows:
- Full batch request JSON
- File upload details
- API responses

## Troubleshooting

### "Resource upload function is disabled"

Enable uploads on the Kyuubi server:

```properties
kyuubi.batch.resource.upload.enabled=true
```

### "required extra resource files [...] are not uploaded"

The filenames in `extraResourcesMap` must match the uploaded file names exactly. This error means the multipart form is missing expected files.

### SSL Certificate Errors

The tool disables SSL verification by default. For production, configure proper certificates.

### File Not Found After Upload

If using `--files` and `SparkFiles.get("filename")` fails, the file may have been renamed by Kyuubi. See [Important Notes for --files](#important-notes-for---files).

### Authentication Failures

- Verify username/password
- Check Kyuubi authentication configuration
- Try with `--debug` to see full error response

## Server Requirements

- Kyuubi 1.8.0+ for basic batch API
- Kyuubi 1.10.0+ for `extraResourcesMap` file upload feature
- Server configuration:
  ```properties
  kyuubi.batch.resource.upload.enabled=true
  kyuubi.frontend.rest.bind.host=0.0.0.0
  ```

## License

MIT License - see [LICENSE](LICENSE) for details.
