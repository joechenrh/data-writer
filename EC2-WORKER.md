# EC2 Worker Architecture

## Overview

```
Your Machine (Web UI)              AWS (us-east-1)
┌──────────────────┐           ┌──────────────────────┐
│  datagen UI      │──create────▶  db9 (tasks table)  │
│  POST /api/create│           └──────────┬───────────┘
└──────────────────┘                      │
                                          │ polls every 30s
                               ┌──────────▼───────────┐
                               │  t3.micro (launcher) │  ~$7.6/month
                               │  on-demand, always-on│
                               └──────────┬───────────┘
                                          │ launches when pending > 0
                               ┌──────────▼────────────┐
                               │  c5.2xlarge (worker)  │  ~$0.12/hour
                               │  spot, one-time       │
                               │  auto-terminate       │
                               └───────────────────────┘
```

## Components

### Launcher (t3.micro, on-demand)
- Instance: `i-0556f75b1eda3e4fb` (Name: `data-writer-launcher`)
- IAM Profile: `data-writer-ec2` (S3 + EC2 permissions)
- Subnet: `subnet-0683981785b0adc1e` (poc-data-gen, has IGW)
- Security Group: `sg-02ec63e73ebf2fa18`
- Key: `data-writer-key`
- Polls db9 every 30s for pending ec2 tasks
- For each pending task, the first claim estimates the total output size and
  reserves N = clamp(total_bytes / 5 TiB, 1, 8) shards on the task row.
- Each subsequent claim hands out the next shard (until all are claimed) and
  the launcher spawns one c5.2xlarge spot worker per shard.
- Pushes its log to S3 every 60s

### Worker (c5.2xlarge, spot one-time)
- Launched automatically by launcher with `-task-id`, `-shard`, `-shard-total`
  flags baked into a per-launch user-data script.
- Downloads data-writer binary from S3.
- Runs `-worker -task-id=X -shard=Y -shard-total=N`: loads the assigned task,
  computes its file slice, and generates only that slice.
- Multiple shards run concurrently across instances and contribute additive
  progress updates to the same task row. The last worker to finish flips the
  task state to `completed`.
- Polls task state every 5s and exits early if the task is cancelled.
- `shutdown -h now` after exit → instance auto-terminates.
- Pushes log to S3 on exit.

### Claim protocol

`data-writer -claim-task` prints one of:
- `<task_id> <shard_idx> <shard_total>` — one shard reserved; spawn a worker.
- `0` — no work available right now.

The launcher script must be redeployed whenever this protocol changes. See
"Restart launcher" below.

## Usage

1. Open web UI at https://datagen.ingresses.org
2. Fill in SQL schema and config
3. Check "Run on EC2"
4. Click Generate
5. Task appears as "pending" → launcher detects within 30s → worker starts → task runs

## Monitoring

### Check task status
```bash
db9 db sql datawriter -q "SELECT id, state, target, files_written, total_files, error FROM tasks ORDER BY id DESC LIMIT 10"
```

### Check launcher log
```bash
aws s3 cp s3://m-poc-dataset/debug/launcher.log /tmp/launcher.log --profile pingcap --region us-east-1 && tail -20 /tmp/launcher.log
```

### Check worker log (after task completes)
```bash
aws s3 ls s3://m-poc-dataset/debug/worker- --profile pingcap --region us-east-1
aws s3 cp s3://m-poc-dataset/debug/worker-<hostname>.log /tmp/worker.log --profile pingcap --region us-east-1 && cat /tmp/worker.log
```

### Check running instances
```bash
aws ec2 describe-instances --profile pingcap --region us-east-1 \
  --filters "Name=tag:Name,Values=data-writer-*" "Name=instance-state-name,Values=running" \
  --query "Reservations[].Instances[].{Id:InstanceId,Type:InstanceType,Name:Tags[?Key=='Name'].Value|[0],State:State.Name}" \
  --output table
```

### Check spot requests (should only see launcher if on-demand)
```bash
aws ec2 describe-spot-instance-requests --profile pingcap --region us-east-1 \
  --filters "Name=state,Values=active,open" \
  --query "SpotInstanceRequests[].{Id:SpotInstanceRequestId,Instance:InstanceId,Type:Type,State:State}" \
  --output table
```

## Troubleshooting

### Task stuck in pending
1. Check launcher is running (see "Check running instances")
2. Check launcher log for errors
3. Verify launcher can reach db9 (network/DNS)

### Worker launched but task not progressing
1. Check worker log in S3
2. Verify worker can reach db9 and S3

### Instance won't die
If a persistent spot instance keeps restarting:
```bash
# 1. Cancel spot request first
aws ec2 describe-spot-instance-requests --profile pingcap --region us-east-1 \
  --filters "Name=state,Values=active,open" --output table
aws ec2 cancel-spot-instance-requests --profile pingcap --region us-east-1 \
  --spot-instance-request-ids <request-id>
# 2. Then terminate
aws ec2 terminate-instances --profile pingcap --region us-east-1 --instance-ids <instance-id>
```

### Restart launcher
```bash
# Terminate old
aws ec2 terminate-instances --profile pingcap --region us-east-1 --instance-ids <old-id>
# Start new (user-data script at /tmp/ec2-launcher.sh)
aws ec2 run-instances --profile pingcap --region us-east-1 \
  --image-id ami-0446b021dec428a7b --instance-type t3.micro \
  --key-name data-writer-key --iam-instance-profile Name=data-writer-ec2 \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=data-writer-launcher}]' \
  --network-interfaces "DeviceIndex=0,SubnetId=subnet-0683981785b0adc1e,Groups=sg-02ec63e73ebf2fa18,AssociatePublicIpAddress=true" \
  --user-data file:///tmp/ec2-launcher.sh \
  --query "Instances[0].InstanceId" --output text
```

### Update data-writer binary
```bash
# Cross-compile
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/data-writer-linux ./src/
# Upload
aws s3 cp /tmp/data-writer-linux s3://m-poc-dataset/tools/data-writer --profile pingcap --region us-east-1
# Restart launcher to pick up new binary
```

## Cost

| Component | Type | Cost |
|-----------|------|------|
| Launcher | t3.micro on-demand | ~$7.6/month |
| Worker | c5.2xlarge spot (per use) | ~$0.12/hour |
| db9 | Serverless Postgres | Free tier |
| **Total (idle)** | | **~$7.6/month** |
| **Total (1h tasks/month)** | | **~$7.7/month** |

## Key Files

| File | Description |
|------|-------------|
| `/tmp/ec2-launcher.sh` | Launcher user-data script |
| `src/server/worker.go` | Worker mode + check-pending |
| `src/server/server.go` | Task worker + DB schema |
| `src/server/handler.go` | API handlers (create with target) |

## AWS Resources

| Resource | Value |
|----------|-------|
| AMI | ami-0446b021dec428a7b (Amazon Linux 2023) |
| IAM Profile | data-writer-ec2 |
| Subnet | subnet-0683981785b0adc1e (poc-data-gen) |
| Security Group | sg-02ec63e73ebf2fa18 |
| Key Pair | data-writer-key |
| S3 Bucket | m-poc-dataset |
| db9 Database | datawriter |
