# MWAA Remote Shell via S3

A pseudo-interactive shell for AWS MWAA (Managed Workflows for Apache Airflow) workers using S3 as a command/response bridge.

## Why This Exists

AWS MWAA runs on **AWS Fargate containers**, which means:
- ❌ No SSH access (Fargate containers don't accept inbound connections)
- ❌ No AWS Systems Manager (SSM) access (Fargate doesn't have EC2 instance IDs)
- ❌ No direct network access to workers

This tool provides an **S3-based remote shell** that works around these limitations by using S3 as a communication channel between you and the MWAA worker.

## How It Works

```
┌─────────────┐                    ┌──────────────┐                    ┌─────────────┐
│             │   Upload command   │              │   Poll for new     │             │
│  Your CLI   │ ──────────────────>│  S3 Bucket   │<──────────────────│MWAA Worker  │
│             │                    │              │   commands         │  (Fargate)  │
│             │                    │  commands/   │                    │             │
│             │                    │  results/    │                    │             │
│             │   Download result  │              │   Upload result    │             │
│             │<───────────────────│              │<───────────────────│             │
└─────────────┘                    └──────────────┘                    └─────────────┘
```

### The Process

1. **DAG starts**: Triggers a long-running task (30 minutes) on a MWAA worker
2. **Worker polls S3**: Every 10 seconds, checks for new command files in S3
3. **You upload command**: Write a shell command to a file and upload to S3
4. **Worker executes**: Finds the command, executes it, captures output
5. **Worker uploads result**: Writes stdout/stderr to S3 result file
6. **You download result**: Retrieve the result file from S3

## Architecture

### Components

**`debug_remote_shell.py`** - Airflow DAG with two tasks:
- `setup_s3_folders` - Creates S3 folder structure
- `remote_shell_session` - Main polling loop that executes commands

**S3 Bucket Structure**:
```
s3://your-mwaa-bucket/
└── debug-shell/
    ├── commands/          # You upload command files here
    │   └── command_1234567890.txt
    └── results/           # Worker uploads results here
        └── result_1234567890.txt
```

### Command Flow

```python
# Worker polling loop (simplified)
while time_remaining > 0:
    # 1. List command files in S3
    commands = s3.list_objects(Prefix="debug-shell/commands/")
    
    for command_file in commands:
        # 2. Download command
        command = s3.get_object(Key=command_file)
        
        # 3. Execute command
        result = subprocess.run(command, shell=True, capture_output=True)
        
        # 4. Upload result
        s3.put_object(Key=result_file, Body=result.stdout + result.stderr)
        
        # 5. Delete command file
        s3.delete_object(Key=command_file)
    
    # 6. Wait 10 seconds before next poll
    time.sleep(10)
```

## Quick Start

### 1. Configure the DAG

Edit `dags/debug_remote_shell.py`:

```python
S3_BUCKET = "your-mwaa-bucket-name"  # Change this!
```

### 2. Upload to MWAA

```bash
aws s3 cp dags/debug_remote_shell.py s3://your-mwaa-bucket/dags/
```

### 3. Trigger the DAG

In the Airflow UI:
1. Find the `debug_remote_shell` DAG
2. Click the play button ▶️ to trigger it
3. Wait for the task to start running

### 4. Send Your First Command

```bash
# Create a command file
echo "python3 -c 'import kombu; print(kombu.__version__)'" > /tmp/cmd.txt

# Upload to S3
aws s3 cp /tmp/cmd.txt s3://your-mwaa-bucket/debug-shell/commands/command_$(date +%s).txt
```

### 5. Get the Result

Wait ~15 seconds, then:

```bash
# List results
aws s3 ls s3://your-mwaa-bucket/debug-shell/results/

# Download latest result
aws s3 cp s3://your-mwaa-bucket/debug-shell/results/result_XXXXX.txt -
```

## Usage Examples

### Check Package Versions

```bash
echo "pip list | grep -E 'kombu|celery|airflow'" > /tmp/cmd.txt
aws s3 cp /tmp/cmd.txt s3://your-bucket/debug-shell/commands/command_$(date +%s).txt
```

### Check Environment Variables

```bash
echo "env | grep AIRFLOW | sort" > /tmp/cmd.txt
aws s3 cp /tmp/cmd.txt s3://your-bucket/debug-shell/commands/command_$(date +%s).txt
```

### List DAG Files

```bash
echo "ls -la /usr/local/airflow/dags/" > /tmp/cmd.txt
aws s3 cp /tmp/cmd.txt s3://your-bucket/debug-shell/commands/command_$(date +%s).txt
```

### Check Disk Space

```bash
echo "df -h" > /tmp/cmd.txt
aws s3 cp /tmp/cmd.txt s3://your-bucket/debug-shell/commands/command_$(date +%s).txt
```

### Test Python Imports

```bash
echo "python3 -c 'from celery import Celery; print(\"Celery OK\")'" > /tmp/cmd.txt
aws s3 cp /tmp/cmd.txt s3://your-bucket/debug-shell/commands/command_$(date +%s).txt
```

### Run Health Check

```bash
# Upload the comprehensive health check
aws s3 cp health-check-command.txt s3://your-bucket/debug-shell/commands/command_$(date +%s).txt

# Wait 20 seconds (health check takes longer)
sleep 20

# Get result
aws s3 cp s3://your-bucket/debug-shell/results/$(aws s3 ls s3://your-bucket/debug-shell/results/ | tail -1 | awk '{print $4}') -
```

## Result Format

Each result file contains:

```
Command: <your command>
Hostname: <worker hostname>
Timestamp: <execution time>
Execution Time: <seconds>
Exit Code: <0 for success, non-zero for error>

=== STDOUT ===
<command output>

=== STDERR ===
<error output>

=== END ===
```

## Configuration Options

Edit these variables in `debug_remote_shell.py`:

```python
# S3 Configuration
S3_BUCKET = "your-mwaa-bucket"           # Your MWAA S3 bucket
S3_COMMAND_PREFIX = "debug-shell/commands/"  # Where to look for commands
S3_RESULT_PREFIX = "debug-shell/results/"    # Where to write results

# Session Configuration
duration_minutes = 30    # How long the session runs
poll_interval = 10       # How often to check S3 (seconds)

# Command Execution
timeout=60              # Max time per command (seconds)
```

## Advanced Usage

### Helper Script

Create a local helper script for easier usage:

```bash
cat > ~/mwaa-shell.sh << 'EOF'
#!/bin/bash
BUCKET="your-mwaa-bucket"

# Send command
echo "$1" > /tmp/cmd.txt
aws s3 cp /tmp/cmd.txt s3://$BUCKET/debug-shell/commands/command_$(date +%s).txt
echo "Command sent. Waiting 15 seconds..."
sleep 15

# Get result
echo ""
echo "=== RESULT ==="
aws s3 cp s3://$BUCKET/debug-shell/results/$(aws s3 ls s3://$BUCKET/debug-shell/results/ | tail -1 | awk '{print $4}') -
EOF

chmod +x ~/mwaa-shell.sh
```

Usage:
```bash
~/mwaa-shell.sh "pip list | grep kombu"
~/mwaa-shell.sh "df -h"
~/mwaa-shell.sh "ps aux | grep airflow"
```

### Multiple Commands in One Session

```bash
# Send multiple commands
for cmd in "python3 --version" "pip list | grep kombu" "df -h" "free -h"; do
    echo "$cmd" > /tmp/cmd.txt
    aws s3 cp /tmp/cmd.txt s3://your-bucket/debug-shell/commands/command_$(date +%s).txt
    sleep 2  # Stagger uploads
done

# Wait for all to complete
sleep 20

# Download all results
aws s3 sync s3://your-bucket/debug-shell/results/ ./mwaa-results/
cat ./mwaa-results/*.txt
```

### Continuous Monitoring

```bash
# Monitor CPU every 30 seconds for 5 minutes
for i in {1..10}; do
    echo "top -bn1 | head -20" > /tmp/cmd.txt
    aws s3 cp /tmp/cmd.txt s3://your-bucket/debug-shell/commands/command_$(date +%s).txt
    sleep 30
done
```

## Limitations

### Command Timeout
- Default: 60 seconds per command
- Commands exceeding timeout are killed
- Increase in DAG if needed: `timeout=300`

### Session Duration
- Default: 30 minutes
- After 30 minutes, the task ends
- Trigger DAG again for a new session

### Command Execution
- Commands run as the `airflow` user
- Limited to worker permissions
- Cannot install system packages (Fargate restriction)
- Cannot modify MWAA infrastructure

### Response Time
- ~15 seconds per command (10s poll + execution + upload)
- Not suitable for real-time interaction
- Use for debugging and inspection, not interactive work

### S3 Costs
- Each command = 1 PUT + 1 GET + 1 DELETE
- Each result = 1 PUT + 1 GET
- Minimal cost for debugging sessions
- Clean up old results to avoid storage costs

## Security Considerations

### What You CAN Do
✅ View environment information  
✅ Check installed packages  
✅ Test Python imports  
✅ Read files in accessible directories  
✅ Run shell commands with worker permissions  

### What You CANNOT Do
❌ Modify MWAA infrastructure  
❌ Access other workers directly  
❌ Bypass IAM permissions  
❌ Install system packages (Fargate restriction)  
❌ Modify Airflow configuration  

### Best Practices

1. **Delete DAG after use**
   ```bash
   aws s3 rm s3://your-bucket/dags/debug_remote_shell.py
   ```

2. **Clean up S3 files**
   ```bash
   aws s3 rm s3://your-bucket/debug-shell/ --recursive
   ```

3. **Restrict S3 bucket access**
   - Use IAM policies to limit who can upload commands
   - Enable S3 bucket versioning
   - Enable S3 server-side encryption

4. **Monitor usage**
   - Check CloudWatch logs for command execution
   - Review S3 access logs
   - Set up alerts for unusual activity

5. **Don't expose sensitive data**
   - Avoid commands that print secrets
   - Don't log credentials or API keys
   - Be careful with environment variables

## Troubleshooting

### DAG Not Appearing

**Check if file is uploaded**:
```bash
aws s3 ls s3://your-bucket/dags/debug_remote_shell.py
```

**Check for DAG parsing errors** in Airflow UI:
- Go to Admin → DAG Errors

### Commands Not Executing

**Verify S3 bucket name** in DAG matches your bucket

**Check command file was uploaded**:
```bash
aws s3 ls s3://your-bucket/debug-shell/commands/
```

**Verify task is running** in Airflow UI:
- Check task state is "running" (green)
- View task logs for errors

**Check CloudWatch logs**:
```bash
aws logs tail /aws/mwaa/your-environment/Task --follow
```

### No Results Appearing

**Wait longer** - First poll can take up to 10 seconds

**Check result path**:
```bash
aws s3 ls s3://your-bucket/debug-shell/results/
```

**View result file** - It will show errors if command failed:
```bash
aws s3 cp s3://your-bucket/debug-shell/results/result_XXXXX.txt -
```

### Permission Errors

**Check MWAA execution role** has S3 permissions:
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`
- `s3:ListBucket`

### Task Timeout

**Default execution timeout**: 35 minutes (30 min session + 5 min buffer)

**If task times out early**, check:
- MWAA environment task timeout settings
- Worker resource constraints
- CloudWatch logs for errors

## Comparison with Other Methods

| Feature | S3 Remote Shell | SSM Session Manager | SSH Server |
|---------|-----------------|---------------------|------------|
| **Works on MWAA** | ✅ Yes | ❌ No | ❌ No |
| **Works on Fargate** | ✅ Yes | ❌ No | ❌ No |
| **Setup Complexity** | Low | Medium | High |
| **Response Time** | ~15 sec | Instant | Instant |
| **Interactive** | Pseudo | Full | Full |
| **Special Permissions** | None | SSM IAM | VPC access |
| **Additional Software** | None | Plugin | SSH server |

**Why SSM doesn't work**: Fargate containers don't have EC2 instance IDs  
**Why SSH doesn't work**: Fargate containers can't accept inbound connections  
**Why S3 works**: Uses S3 API, which Fargate containers can access  

See `MWAA_FARGATE_LIMITATIONS.md` for detailed explanation.

## Related Documentation

- **Quick Reference**: `DEBUG_DAGS_QUICK_REFERENCE.md`
- **Detailed Guide**: `INTERACTIVE_SHELL_GUIDE.md`
- **Usage Examples**: `DEBUG_DAGS_USAGE_GUIDE.md`
- **Fargate Limitations**: `MWAA_FARGATE_LIMITATIONS.md`
- **Health Check Commands**: `HEALTH_CHECK_COMMANDS.md`

## Technical Details

### DAG Structure

```python
with DAG('debug_remote_shell', ...) as dag:
    setup = PythonOperator(
        task_id='setup_s3_folders',
        python_callable=setup_s3_folders,
    )
    
    shell = PythonOperator(
        task_id='remote_shell_session',
        python_callable=remote_shell_session,
        execution_timeout=timedelta(minutes=35),
    )
    
    setup >> shell
```

### Key Functions

**`setup_s3_folders()`**
- Creates placeholder files in S3 to ensure folders exist
- Validates S3 bucket access
- Runs before main session

**`remote_shell_session()`**
- Main polling loop
- Lists command files in S3
- Downloads and executes commands
- Uploads results
- Deletes processed commands
- Runs for 30 minutes with heartbeat

### Error Handling

- **Command timeout**: 60 seconds, then killed
- **S3 errors**: Logged and skipped, polling continues
- **Execution errors**: Captured in result file with exit code
- **Bucket access errors**: Task fails immediately with helpful message

### Performance

- **Poll interval**: 10 seconds (configurable)
- **Command execution**: Parallel to polling (non-blocking)
- **S3 operations**: Minimal (1-2 per command)
- **Worker impact**: Low (mostly sleeping)

## FAQ

### Q: How long does it take to get results?

**A**: ~15 seconds. The worker polls every 10 seconds, plus execution time and S3 upload time.

### Q: Can I run multiple commands at once?

**A**: Yes! Upload multiple command files. The worker processes them sequentially in the order it finds them.

### Q: What happens if a command fails?

**A**: The result file will show the error in STDERR and a non-zero exit code. The worker continues processing other commands.

### Q: Can I run long-running commands?

**A**: Yes, but they timeout after 60 seconds by default. Increase the timeout in the DAG if needed.

### Q: Is this secure?

**A**: Yes, as long as you:
- Restrict S3 bucket access with IAM policies
- Enable S3 encryption
- Clean up command/result files after debugging
- Delete the DAG when done

### Q: Can I use this in production?

**A**: It's designed for debugging, not production use. For production monitoring, use CloudWatch Logs and metrics instead.

### Q: Why not just use CloudWatch Logs?

**A**: CloudWatch Logs are great for viewing output, but this tool lets you run arbitrary commands interactively without redeploying your DAGs.

### Q: Does this work with MWAA 1.x?

**A**: Yes! It works with any MWAA version (1.x, 2.x) because it only uses S3 and standard Python libraries.

### Q: Can I use this with self-hosted Airflow?

**A**: Yes, but only if your workers can't be accessed directly. If you have SSH access to your workers, just use SSH instead.

## Contributing

Found a bug or have a suggestion? This is a debugging tool for MWAA environments. Feel free to modify it for your needs!

## License

This is a debugging tool provided as-is. Use at your own risk. Always test in a development environment first.

---

**Remember**: This tool is specifically designed for AWS MWAA's Fargate architecture where traditional remote access methods don't work. It's the only interactive debugging method that works reliably on MWAA! 🎯
