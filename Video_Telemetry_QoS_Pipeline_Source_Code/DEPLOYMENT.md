# Deployment Guide

> **Automated deployment instructions for Video Telemetry QoS Analytics Pipeline**

## Prerequisites

### 1. Install Databricks CLI

```bash
# Install via pip
pip install databricks-cli

# Verify installation
databricks --version
```

### 2. Configure Authentication

**Option A: Using Personal Access Token (recommended for development)**

```bash
# Configure interactively
databricks configure --token

# You'll be prompted for:
# - Databricks Host: https://dbc-a1e55473-3f27.cloud.databricks.com
# - Token: <your-personal-access-token>
```

**Option B: Using Environment Variables**

```bash
export DATABRICKS_HOST="https://dbc-a1e55473-3f27.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-token>"
```

**Option C: Using Service Principal (recommended for production)**

```bash
export DATABRICKS_HOST="https://dbc-a1e55473-3f27.cloud.databricks.com"
export DATABRICKS_CLIENT_ID="<service-principal-client-id>"
export DATABRICKS_CLIENT_SECRET="<service-principal-client-secret>"
```

### 3. Verify Permissions

Ensure you have:
- **Workspace access**: Can create notebooks, files, and jobs
- **Unity Catalog permissions**:
  - `USE CATALOG` on `workspace`
  - `USE SCHEMA` on `workspace.hive_video_analytics`
  - `CREATE TABLE` on schema
- **Pipeline creation permissions**: Can create and manage pipelines

---

## Deployment Targets

The bundle supports three environments:

| Environment | Target | Schema | Use Case |
|-------------|--------|--------|----------|
| **Dev** | `dev` | `hive_video_analytics_dev` | Development and testing |
| **Staging** | `staging` | `hive_video_analytics_staging` | Pre-production validation |
| **Prod** | `prod` | `hive_video_analytics` | Production workloads |

---

## Deployment Commands

### Deploy to Development (Default)

```bash
# Navigate to pipeline directory
cd /Users/vasuambi@gmail.com/video_telemetry_qos_analytics_pipeline_1ff1d722

# Validate bundle configuration
databricks bundle validate -t dev

# Deploy all resources (pipeline + jobs)
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run video_qos_pipeline -t dev
```

### Deploy to Staging

```bash
# Validate
databricks bundle validate -t staging

# Deploy
databricks bundle deploy -t staging

# Run
databricks bundle run video_qos_pipeline -t staging
```

### Deploy to Production

```bash
# Validate (includes additional checks for production)
databricks bundle validate -t prod

# Deploy (requires service principal in production)
databricks bundle deploy -t prod

# Run pipeline
databricks bundle run video_qos_pipeline -t prod
```

---

## What Gets Deployed

### 1. Pipeline Resource

**Name**: `video_telemetry_qos_pipeline` (with `_dev`, `_staging`, `_prod` suffix)

**Configuration**:
- **Compute**: Serverless with Photon
- **Target schema**: Environment-specific
- **Libraries**: 4 notebooks (bronze, silver x2, gold)
- **Clustering**: Liquid clustering enabled
- **Optimization**: Auto-compact and optimize write

### 2. Scheduled Jobs (3 jobs)

#### Daily Pipeline Run
- **Schedule**: 2 AM UTC daily
- **Task**: Incremental pipeline refresh
- **Timeout**: 2 hours
- **Notifications**: Email on failure

#### Weekly Pipeline Tests
- **Schedule**: Sundays 4 AM UTC
- **Task**: Run automated test suite
- **Timeout**: 1 hour
- **Notifications**: Email on failure

#### Monthly Retention Cleanup
- **Schedule**: 1st of month, 3 AM UTC
- **Tasks**: 4 sequential cleanup tasks (bronze → silver → quarantine → gold)
- **Timeout**: 2 hours
- **Notifications**: Email on success and failure

---

## First-Time Setup

### After Initial Deployment

1. **Create Unity Catalog schema** (if it doesn't exist):

```sql
CREATE SCHEMA IF NOT EXISTS workspace.hive_video_analytics;

-- For dev environment
CREATE SCHEMA IF NOT EXISTS workspace.hive_video_analytics_dev;

-- For staging environment
CREATE SCHEMA IF NOT EXISTS workspace.hive_video_analytics_staging;
```

2. **Run full refresh** (first time only):

```bash
# Via bundle
databricks bundle run video_qos_pipeline -t prod

# Or via CLI with full refresh
databricks pipelines start-update \
  --pipeline-id <pipeline-id-from-deployment> \
  --full-refresh
```

3. **Verify pipeline execution**:

```bash
# Get pipeline ID from deployment output
databricks pipelines get --pipeline-id <pipeline-id>

# Check pipeline status
databricks pipelines list-updates --pipeline-id <pipeline-id>
```

4. **Verify tables were created**:

```sql
SHOW TABLES IN workspace.hive_video_analytics;

-- Expected tables:
-- bronze_telemetry_raw
-- silver_telemetry_enriched
-- silver_telemetry_quarantine
-- gold_viewer_qos_metrics
```

5. **Check scheduled jobs**:

```bash
# List all jobs in workspace
databricks jobs list --output json | jq '.jobs[] | select(.settings.name | contains("video_telemetry"))'

# You should see 3 jobs:
# - video_telemetry_qos_pipeline_daily_run
# - video_telemetry_qos_pipeline_weekly_tests
# - video_telemetry_qos_pipeline_monthly_cleanup
```

---

## Updating Existing Deployment

### Update Pipeline Configuration

```bash
# Make changes to bundle.yml or transformation notebooks

# Validate changes
databricks bundle validate -t prod

# Deploy updated configuration
databricks bundle deploy -t prod

# Pipeline will use new configuration on next run
```

### Update Transformation Logic

```bash
# Edit transformation notebooks in transformations/ directory

# Re-deploy bundle (pushes updated notebooks)
databricks bundle deploy -t prod

# Run pipeline with updated transformations
databricks bundle run video_qos_pipeline -t prod
```

### Update Job Schedules

```bash
# Edit bundle.yml job definitions (schedule, tasks, etc.)

# Deploy changes
databricks bundle deploy -t prod

# Jobs will automatically use new schedules
```

---

## Managing Jobs

### Pause/Resume Scheduled Jobs

```bash
# Get job ID
JOB_ID=$(databricks jobs list --output json | jq -r '.jobs[] | select(.settings.name == "video_telemetry_qos_pipeline_daily_run") | .job_id')

# Pause job
databricks jobs update $JOB_ID --json '{
  "new_settings": {
    "schedule": {
      "pause_status": "PAUSED"
    }
  }
}'

# Resume job
databricks jobs update $JOB_ID --json '{
  "new_settings": {
    "schedule": {
      "pause_status": "UNPAUSED"
    }
  }
}'
```

### Manually Run Jobs

```bash
# Run daily pipeline job
databricks jobs run-now --job-id <job-id>

# Run with specific parameters
databricks jobs run-now --job-id <job-id> \
  --notebook-params '{"full_refresh": "true"}'
```

### View Job Runs

```bash
# List recent runs for a job
databricks jobs runs list --job-id <job-id> --limit 10

# Get details of a specific run
databricks jobs runs get --run-id <run-id>

# Get run output
databricks jobs runs get-output --run-id <run-id>
```

---

## Monitoring Deployment

### Check Pipeline Status

```bash
# Get pipeline details
databricks pipelines get --pipeline-id <pipeline-id>

# Get latest update
databricks pipelines list-updates --pipeline-id <pipeline-id> --max-results 1

# Get event logs
databricks pipelines list-pipeline-events --pipeline-id <pipeline-id> --max-results 50
```

### Check Data Quality

```sql
-- Record counts per layer
SELECT 'Bronze' as layer, COUNT(*) as record_count 
FROM workspace.hive_video_analytics.bronze_telemetry_raw
UNION ALL
SELECT 'Silver', COUNT(*) 
FROM workspace.hive_video_analytics.silver_telemetry_enriched
UNION ALL
SELECT 'Quarantine', COUNT(*) 
FROM workspace.hive_video_analytics.silver_telemetry_quarantine
UNION ALL
SELECT 'Gold', COUNT(*) 
FROM workspace.hive_video_analytics.gold_viewer_qos_metrics;

-- Data freshness check
SELECT 
  'Bronze' as layer,
  MAX(eventDate) as latest_date,
  DATEDIFF(CURRENT_DATE(), MAX(eventDate)) as days_behind
FROM workspace.hive_video_analytics.bronze_telemetry_raw
UNION ALL
SELECT 'Silver', MAX(eventDate), DATEDIFF(CURRENT_DATE(), MAX(eventDate))
FROM workspace.hive_video_analytics.silver_telemetry_enriched
UNION ALL
SELECT 'Gold', MAX(eventDate), DATEDIFF(CURRENT_DATE(), MAX(eventDate))
FROM workspace.hive_video_analytics.gold_viewer_qos_metrics;
```

---

## Rollback Procedure

### Rollback Pipeline Changes

```bash
# If deployment fails or causes issues:

# 1. Check git history
git log --oneline

# 2. Revert to previous version
git revert <commit-hash>

# 3. Re-deploy
databricks bundle deploy -t prod
```

### Rollback to Specific Data Version

```sql
-- Delta Lake time travel (restore table to previous version)
RESTORE TABLE workspace.hive_video_analytics.silver_telemetry_enriched 
TO VERSION AS OF 123;

-- Or restore to specific timestamp
RESTORE TABLE workspace.hive_video_analytics.silver_telemetry_enriched 
TO TIMESTAMP AS OF '2026-06-01T12:00:00';

-- Check version history
DESCRIBE HISTORY workspace.hive_video_analytics.silver_telemetry_enriched;
```

---

## Troubleshooting Deployment

### Common Issues

#### 1. "Bundle validation failed"

**Cause**: Syntax error in `bundle.yml`

**Solution**:
```bash
# Check YAML syntax
yamllint bundle.yml

# Or use online YAML validator
# Review error message for line number and issue
```

#### 2. "Insufficient permissions"

**Cause**: Missing Unity Catalog or workspace permissions

**Solution**:
```sql
-- Grant required permissions
GRANT USE CATALOG ON CATALOG workspace TO `<your-user>`;
GRANT USE SCHEMA ON SCHEMA workspace.hive_video_analytics TO `<your-user>`;
GRANT CREATE TABLE ON SCHEMA workspace.hive_video_analytics TO `<your-user>`;
```

#### 3. "Pipeline not found"

**Cause**: Pipeline ID changed or deployment failed

**Solution**:
```bash
# List all pipelines
databricks pipelines list

# Find pipeline by name
databricks pipelines list --output json | jq '.statuses[] | select(.name | contains("video_qos"))'

# Update bundle with correct pipeline ID
```

#### 4. "Job creation failed"

**Cause**: Invalid cluster configuration or notebook path

**Solution**:
```bash
# Check notebook paths exist
ls -la transformations/bronze/
ls -la operations/

# Verify cluster configuration in bundle.yml
# Ensure spark_version and node_type_id are valid
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
# .github/workflows/deploy.yml
name: Deploy Video QoS Pipeline

on:
  push:
    branches:
      - main  # Deploy to prod on main branch
      - staging  # Deploy to staging on staging branch
      - dev  # Deploy to dev on dev branch

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install Databricks CLI
        run: pip install databricks-cli
      
      - name: Configure Databricks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          echo "[DEFAULT]" > ~/.databrickscfg
          echo "host = $DATABRICKS_HOST" >> ~/.databrickscfg
          echo "token = $DATABRICKS_TOKEN" >> ~/.databrickscfg
      
      - name: Determine target environment
        id: target
        run: |
          if [[ "${{ github.ref }}" == "refs/heads/main" ]]; then
            echo "target=prod" >> $GITHUB_OUTPUT
          elif [[ "${{ github.ref }}" == "refs/heads/staging" ]]; then
            echo "target=staging" >> $GITHUB_OUTPUT
          else
            echo "target=dev" >> $GITHUB_OUTPUT
          fi
      
      - name: Validate bundle
        run: databricks bundle validate -t ${{ steps.target.outputs.target }}
      
      - name: Deploy bundle
        run: databricks bundle deploy -t ${{ steps.target.outputs.target }}
      
      - name: Run tests (dev/staging only)
        if: steps.target.outputs.target != 'prod'
        run: |
          JOB_ID=$(databricks jobs list --output json | jq -r '.jobs[] | select(.settings.name | contains("weekly_tests")) | .job_id')
          databricks jobs run-now --job-id $JOB_ID
```

---

## Environment-Specific Configuration

### Development Environment

```yaml
# bundle.yml (dev target)
targets:
  dev:
    mode: development  # Enables development mode features
    variables:
      catalog: workspace
      schema: hive_video_analytics_dev  # Isolated dev schema
      pipeline_name: video_telemetry_qos_pipeline_dev
```

**Features**:
- Development mode enabled (better error messages, relaxed validation)
- Isolated schema to avoid interfering with production
- Faster iteration cycle

### Production Environment

```yaml
# bundle.yml (prod target)
targets:
  prod:
    mode: production  # Stricter validation
    run_as:
      service_principal_name: "${var.prod_service_principal}"
    variables:
      catalog: workspace
      schema: hive_video_analytics  # Production schema
      pipeline_name: video_telemetry_qos_pipeline
```

**Features**:
- Production mode (stricter validation, optimized performance)
- Service principal for secure deployments
- Production-grade configurations

---

## Next Steps

After successful deployment:

1. ✅ **Verify pipeline runs**: Check that daily job executes successfully
2. ✅ **Set up monitoring alerts**: Configure email/Slack notifications
3. ✅ **Run initial tests**: Execute weekly test suite manually
4. ✅ **Monitor data quality**: Check quarantine rates and freshness
5. ✅ **Document runbook**: Add incident response procedures
6. ✅ **Train team**: Share deployment and troubleshooting guides

---

## Support

**For deployment issues**:
- Check this guide's troubleshooting section
- Review Databricks logs in workspace UI
- Contact: vasuambi@gmail.com

**Documentation**:
- [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html)
- [Unity Catalog Permissions](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html)
