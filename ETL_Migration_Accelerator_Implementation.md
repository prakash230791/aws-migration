# ETL Migration Accelerator
## Custom Tool — Implementation Design Document

**SSIS + ADF → AWS Glue Python Shell | PySpark | Visual ETL**  
**Source: Azure SQL MI → Target: AWS RDS SQL Server**  
*Enterprise Architecture | June 2026 | v3.0 | VS Code Accelerator for ETL Leads & Developers*

---

| Attribute | Value |
|---|---|
| Document type | Implementation Design — builder's guide for constructing the Migration Accelerator tool |
| Scale | 14 applications × 100+ ETL jobs each = 1,400+ jobs total |
| Source packages | SSIS .dtsx + .conmgr + .params \| ADF Pipeline JSON + Dataset JSON + Linked Service JSON |
| Source database | Azure SQL Managed Instance (SQL Server engine) |
| Target database | AWS RDS for SQL Server (same engine — lift-and-shift, no schema conversion) |
| Target ETL outputs | AWS Glue Python Shell jobs \| AWS Glue PySpark jobs \| AWS Glue Visual ETL (flagged, manual config) |
| Primary AI tool | GitHub Copilot (GHCP) in VS Code — no direct LLM API key available; tool prepares inputs for GHCP |
| Deployment runtime | Python script using AWS boto3 SDK — no IaC required for primary path |
| Developer environment | VS Code with GHCP extension — tool runs as CLI commands within VS Code terminal |
| Version | v3.0 — ground-up redesign based on real estate (TRD Migration Factory Suite + 14-app scale) |

> **Design Philosophy**
> - The tool is a **structure engine, not an intelligence engine**. It handles parsing, classification, connection mapping, infrastructure dependency resolution, and deployment. GHCP handles code generation. Neither trespasses on the other's domain.
> - The **FSD (Functional Specification Document) is the source of truth**. Everything downstream — GHCP prompt, generated script, deployment configuration — derives from the SME-approved FSD.
> - The tool is a **factory that runs 14 times**, once per application. Application-specific configuration drives connection strings, secret names, S3 paths, IAM roles, and VPC settings — never hardcoded in generated scripts.
> - The **SME bottleneck is the critical constraint** at this scale. The tool minimises SME time: batch census review, confidence scoring, and bulk approval of high-confidence zero-manual-item jobs.

---

## Contents

1. [Five-Step Workflow](#1-five-step-workflow)
2. [Repository Structure](#2-repository-structure)
3. [Application Configuration](#3-application-configuration)
4. [Target Type Classification](#4-target-type-classification)
5. [Parser Specification](#5-parser-specification)
6. [Infrastructure Dependency Handling](#6-infrastructure-dependency-handling)
7. [FSD Schema — Functional Specification Document](#7-fsd-schema)
8. [GHCP Prompt Structure](#8-ghcp-prompt-structure)
9. [Prerequisites Manifest](#9-prerequisites-manifest)
10. [QA Checks — Pre-Deployment Validation](#10-qa-checks)
11. [Deployment — tool.py deploy](#11-deployment)
12. [Programme Dashboard](#12-programme-dashboard)
13. [Sprint Build Plan](#13-sprint-build-plan)
14. [Developer Implementation Sessions — GHCP Prompt Guide](#14-developer-implementation-sessions)
15. [Fallback Plan](#15-fallback-plan)

---

## 1. Five-Step Workflow

The tool operates as a five-step pipeline. Steps 1, 2, and 5 are fully automated. Step 3 is a human gate — the SME validates and approves the FSD. Step 4 is GHCP-assisted — the tool prepares a structured prompt that the developer pastes into GHCP in VS Code.

| Step | Name | Who | Tool Command | Input | Output |
|---|---|---|---|---|---|
| 1 | DROP | ETL Developer | Manual | SSIS .dtsx / ADF JSON files | Files placed in `source-packages/{app}/` |
| 2 | PARSE | Tool (automated) | `python tool.py parse --app {app}` | All source files | FSD per job in `fsd/{app}/` \| `census.csv` \| prereqs manifest |
| 3 | VALIDATE | SME + ETL Developer | Manual (VS Code) | FSD Markdown files | FSD with `approved: true` \| All `[MANUAL]` items filled |
| 4 | GENERATE | GHCP in VS Code | `python tool.py prompt --app {app} --job {name}` | Approved FSD | Structured GHCP prompt → developer pastes into GHCP → GHCP generates script |
| 5 | DEPLOY | Tool (automated) | `python tool.py deploy --app {app} --job {name}` | Generated Python script + FSD config | QA report \| Script on S3 \| Glue job created \| EventBridge rule created |

> **Why GHCP and not a direct LLM API call**  
> No shared LLM API key is available. GHCP in VS Code is the LLM access path. This is not a limitation — it keeps a human in the generation loop, ensures code is reviewed before being saved, and uses GHCP's VS Code context awareness (it can see adjacent files, the base template, and the project structure).

---

## 2. Repository Structure

```
migration-accelerator/              ← VS Code workspace root
├── tool.py                         ← single CLI entry point (all commands)
├── requirements.txt                ← boto3, pandas, sqlalchemy, pymssql, pyodbc, pyyaml, jinja2
├── .env.example                    ← AWS credentials template (never commit .env)
│
├── config/
│   ├── apps/                       ← one YAML file per application
│   │   ├── app_crm.yaml
│   │   ├── app_finance.yaml
│   │   └── ... (14 total)
│   ├── classification_rules.yaml   ← override rules for target type classification
│   ├── connection_type_map.yaml    ← ADF/SSIS connection type → role mapping
│   └── base_templates/
│       ├── python_shell_base.py    ← base template GHCP builds on (Python Shell)
│       ├── pyspark_base.py         ← base template for PySpark jobs
│       └── visual_etl_checklist.md ← manual checklist for Visual ETL candidates
│
├── source-packages/                ← Step 1: drop source files here
│   ├── crm/
│   │   ├── ssis/                   ← .dtsx, .conmgr, .params files
│   │   └── adf/                    ← pipeline/, dataset/, linkedService/ folders
│   └── ... (one folder per app)
│
├── fsd/                            ← Step 2 output / Step 3 input
│   └── crm/
│       └── etl_orders_daily.md     ← FSD per job (tool generates, SME edits)
│
├── prompts/                        ← Step 4 input (developer pastes into GHCP)
│   └── crm/
│       └── etl_orders_daily_prompt.md
│
├── generated/                      ← Step 4 output (developer saves GHCP output here)
│   └── crm/
│       └── etl_orders_daily.py
│
├── deployed/                       ← Step 5: tool copies here after successful deploy
│   └── crm/
│       └── etl_orders_daily.py
│
├── reports/
│   ├── crm_census.xlsx             ← per-app classification report
│   ├── crm_prereqs.txt             ← per-app infrastructure prerequisites
│   ├── crm_qa_report.txt           ← per-app QA results
│   └── programme_dashboard.xlsx    ← cross-app progress rollup (all 14 apps)
│
└── tool/                           ← tool source code
    ├── __init__.py
    ├── cli.py                      ← argparse CLI
    ├── config_loader.py            ← loads app_{code}.yaml → AppConfig dataclass
    ├── parser/
    │   ├── ssis_parser.py          ← SSIS .dtsx XML parser
    │   ├── adf_parser.py           ← ADF JSON parser (resolves dataset→linkedService chain)
    │   ├── connection_classifier.py
    │   ├── pattern_detector.py     ← three-phase pattern, SP dependency graph
    │   └── target_classifier.py    ← PYTHON_SHELL | PYSPARK | VISUAL_ETL
    ├── fsd/
    │   └── fsd_writer.py
    ├── prompt/
    │   └── prompt_generator.py
    ├── qa/
    │   └── qa_checker.py
    ├── deployer/
    │   ├── s3_uploader.py
    │   ├── glue_deployer.py
    │   ├── eventbridge_deployer.py
    │   └── crawler_deployer.py
    └── reporting/
        ├── census.py
        ├── prereqs.py
        └── dashboard.py
```

---

## 3. Application Configuration

### 3.1 App Config Schema

Each of the 14 applications has its own YAML file in `config/apps/`. Generated scripts never hardcode any values — everything comes from this file.

```yaml
# config/apps/app_crm.yaml
app_name: CRM
app_code: crm

# IN_MIGRATION: source reads Azure SQL MI, writes to RDS
# POST_CUTOVER: both source and target point to RDS SQL Server
migration_stage: IN_MIGRATION

source:
  type: AZURE_SQL_MI
  secret_name: secret/crm/source-azure-sql-mi
  database: CRM_DB
  schema: dbo

target:
  type: AWS_RDS_SQL_SERVER
  secret_name: secret/crm/target-rds-sql-server
  database: CRM_DB
  schema: dbo
  staging_schema: staging

# AZURE_SQL_MI: SPs still on source | RDS: SPs migrated to RDS
sp_execution_target: RDS

aws_region: eu-west-1
glue_iam_role: arn:aws:iam::123456789:role/GlueRole-CRM
vpc_connection_source: glue-conn-crm-source-azure-sql
vpc_connection_target: glue-conn-crm-target-rds
vpc_connection_name: glue-vpc-crm
s3_scripts_bucket: s3://glue-scripts-prod/crm/
s3_data_bucket: s3://data-lake-prod/crm/
sns_alert_topic: arn:aws:sns:eu-west-1:123456789:etl-alerts-crm
glue_catalog_database: crm_catalog

# Maps SSIS connection manager names and ADF linked service names
# to their role. Unknown names → flagged [MANUAL] in FSD.
connection_mapping:
  "Azure SQL MI - CRM":   source
  "OLEDB_CRM_Source":     source
  "OLEDB_CRM_Staging":    target
  "LS_AzureSqlMI_CRM":   source
  "LS_RdsSqlServer_CRM":  target
  "LS_SFTP_CRM":          sftp

python_shell:
  python_version: "3.9"      # AWS Glue Python Shell requires exactly 3.9
  max_capacity: 1.0           # 1 DPU = 16 GB RAM
  chunksize: 50000            # pandas read_sql chunksize — never change
  additional_python_modules: "pandas,sqlalchemy,pymssql,oracledb"

source_packages_path: source-packages/crm/
fsd_output_path: fsd/crm/
prompt_output_path: prompts/crm/
generated_path: generated/crm/
deployed_path: deployed/crm/
```

### 3.2 Migration Stage — Dual Connection Pattern

| Migration Stage | Source reads from | Target writes to | SP calls go to | When to use |
|---|---|---|---|---|
| `IN_MIGRATION` | Azure SQL MI | RDS SQL Server | RDS SQL Server | Default during programme — active DMS CDC window |
| `POST_CUTOVER` | RDS SQL Server | RDS SQL Server | RDS SQL Server | After application cutover — source and target are same instance |
| `COMPLETED` | RDS SQL Server | RDS SQL Server | RDS SQL Server | Azure resources decommissioned for this application |

When an application completes cutover: change `migration_stage: POST_CUTOVER` and rerun `tool.py deploy --app {app} --all`. Scripts redeploy with single-connection pattern automatically.

---

## 4. Target Type Classification

| Target Type | When to use | Stack | DPU | Est. % of estate |
|---|---|---|---|---|
| `VISUAL_ETL` | Simple source→filter→target. No SPs. No complex expressions. No Script Components. Max 2 sources. Row count < 500K. Max 2 transforms. | AWS Glue Visual ETL — console configuration, not code-generated | G.025X, 2 workers | 10–15% |
| `PYTHON_SHELL` | SP wrapper pattern. Simple data movement with pandas viable (< 5GB). REST/HTTP calls. File operations. Copy Activity with minimal transformation. | Python 3.9 \| pandas + sqlalchemy (reads) \| pymssql (DDL + DML) \| boto3 | 1.0 DPU (16GB RAM) | 40–50% |
| `PYSPARK` | Data volume > 5GB. Complex multi-table joins. Aggregations / window functions. Conditional splits. SSIS Data Flow with Lookup + Derived Column + Aggregate. Row count > 5M. | PySpark 3.x \| GlueContext \| spark.read.jdbc | G.1X or G.2X, 2–4 workers | 35–45% |

### 4.1 Classification Logic

```python
def classify_target(job: ParsedJob, app_config: AppConfig) -> GlueTarget:
    # SP wrapper pattern — highest priority
    if job.has_stored_procedure_calls and job.row_count_estimate < 5_000_000:
        return GlueTarget.PYTHON_SHELL

    # Large volume
    if job.row_count_estimate > 5_000_000 or job.source_size_gb > 5.0:
        return GlueTarget.PYSPARK

    # Complex transformations
    complex_transforms = [
        job.has_window_functions,
        job.has_multi_table_joins,        # more than 2 tables
        job.has_aggregations,
        job.has_conditional_split,
        job.has_ssis_script_component,
        job.has_adf_mapping_dataflow_complex,  # > 4 transforms
    ]
    if any(complex_transforms):
        return GlueTarget.PYSPARK

    # Simple pattern — Visual ETL candidate
    if (not job.has_stored_procedure_calls
            and not job.has_complex_expressions
            and not job.has_ssis_script_component
            and job.source_count <= 2
            and job.transform_count <= 2
            and job.row_count_estimate < 500_000):
        return GlueTarget.VISUAL_ETL

    return GlueTarget.PYTHON_SHELL  # default
```

---

## 5. Parser Specification

### 5.1 SSIS Parser

Reads `.dtsx` (main package), `.conmgr` (connection managers), `.params` (parameters). All three must be in `source-packages/{app}/ssis/`.

| XML Element | Extraction target | FSD section | Notes |
|---|---|---|---|
| `DTS:ConnectionManager/@DTS:ObjectName` + ConnectionString | Connection name + server/database/auth | §2 Data Sources | Run through `connection_classifier.py`. Flag unknowns `[MANUAL]`. |
| `DTS:Parameter` — name, DataType, DefaultValue | Package parameters | §9 Parameters | → `getResolvedOptions` arguments |
| `DTS:PackageVariable` — name, DataType, Value | Package variables | §9 Parameters | Classify: JOB_PARAMETER \| COMPUTED \| CONSTANT \| SYSTEM_VARIABLE |
| `DTSAdapter.OleDbSource` | Source table/query, column list | §2 Data Sources | Extract `SqlCommand` property; resolve connection name to role |
| `DTSTransform.Lookup` | Lookup keys, output columns, no-match behaviour | §4 Transformations | No-match disposition drives PYSPARK classifier |
| `DTSTransform.DerivedColumn` | Output column name + expression | §4 Transformations | Pass to `expression_normaliser.py`. Unknown → `[MANUAL]`. |
| `DTSTransform.ConditionalSplit` | Conditions in priority order | §4 Transformations | Overlapping conditions → `[MANUAL: VERIFY CONDITION COVERAGE]` |
| `Microsoft.SqlServer.Dts.Tasks.ScriptTask` | Script Component body (C#/VB) | §4 Transformations | Flag `[MANUAL: SCRIPT COMPONENT]`. Include structure, exclude body. |
| `Microsoft.ExecuteSQLTask` | SQL statement, connection ref | §4 / §5 SP Dependency | `EXEC sp_name` → SP call. `SELECT` → source. `DML` → transform. |
| `DTS:EventHandler[@EventHandlerType="OnError"]` | Error handler actions | §7 Error Handling | Map to SNS alert pattern |
| `DTS:PrecedenceConstraint` | Task dependencies (from/to) | §6 Control Flow | Build adjacency list → SP dependency graph → three-phase detection |

### 5.2 ADF Parser

ADF exports have three subfolders: `pipeline/`, `dataset/`, `linkedService/`. The parser must traverse all three.

| JSON Path | Extraction target | FSD section | Notes |
|---|---|---|---|
| `properties.activities[*].name + .type` | All activity names and types | §6 Control Flow | Type maps via `connection_type_map.yaml` |
| `activities[?type=="Copy"].typeProperties.source + .sink` | Copy source + sink | §2 Sources + §3 Targets | Resolve dataset → linkedService chain to get table names |
| `activities[?type=="ExecuteDataFlow"].dataFlow.referenceName` | Data Flow name → separate JSON | §4 Transformations | Parse separate Data Flow JSON; extract transformation nodes |
| `dataflow.properties.transformations[*].type + .name` | Data Flow transform nodes | §4 Transformations | Type determines complexity → feeds classifier |
| `activities[?type=="SqlServerStoredProcedure"].typeProperties` | SP name + parameters | §4 / §5 SP Dependency | Build SP dependency graph node |
| `activities[?type=="ForEach"].typeProperties.items + .activities` | ForEach iterator + inner activities | §6 Control Flow | `[MANUAL: FOREACH LOOP BODY]`. Glue Workflows has no native ForEach — logic moves inside the job. |
| `activities[?type=="IfCondition"].typeProperties.expression` | If condition + true/false branches | §6 Control Flow | `[MANUAL: IF CONDITION]`. Glue Workflows has no native if/else on data content. |
| `activities[?type=="WebActivity"].typeProperties.url + .method` | HTTP URL, method | §4 Transformations | → Python `requests` in generated script + VPC egress check |
| `triggers[*].name + .type + .properties` | All trigger definitions | §8 Schedule & Triggers | Schedule → EventBridge cron (with ADF→AWS cron translation) |
| `properties.parameters[*]` | Pipeline parameters | §9 Parameters | → `getResolvedOptions` arguments |

### 5.3 Three-Phase Pattern Detection

> **Three-Phase Static Load** is the dominant pattern in SSIS estates.
>
> - **Phase 1 (Extract):** Execute SQL Tasks with TRUNCATE at start + OLEDB Source reads writing to staging tables (names containing `staging_`, `tmp_`, `stg_`, or `_new` suffix)
> - **Phase 2 (SP Execution):** Execute SQL Tasks calling stored procedures in a defined order. The SP precedence constraint adjacency list becomes the SP dependency graph.
> - **Phase 3 (Transfer):** Execute SQL Tasks with `INSERT INTO main SELECT * FROM staging`, or `MERGE` statements, or table swap via `sp_rename`.
>
> If all three phases detected: `pattern: THREE_PHASE_STATIC_LOAD`  
> If not detected: `pattern: GENERAL_ETL`

### 5.4 SP Dependency Graph

```markdown
## 5. SP Dependency Graph (extracted from precedence constraints)

Level 1 — Run first (no dependencies):
  - dbo.sp_validate_orders
  - dbo.sp_load_reference_data    ← parallel with sp_validate_orders

Level 2 — Run after Level 1 completes:
  - dbo.sp_enrich_orders           ← depends on: sp_validate_orders
  - dbo.sp_calculate_pricing       ← depends on: sp_load_reference_data

Level 3 — Run after Level 2 completes:
  - dbo.sp_finalise_orders         ← depends on: sp_enrich_orders, sp_calculate_pricing

SP execution target: TARGET (RDS SQL Server)
Use target_dml_conn for all SP calls.
```

---

## 6. Infrastructure Dependency Handling

### 6.1 Linked Services / Connection Managers → Glue Connections

| Source object | AWS equivalent | Tool action | Status |
|---|---|---|---|
| AzureSqlMI Linked Service (source) | AWS Glue JDBC Connection → Azure SQL MI | Parser extracts name → app config maps to source role → prereqs manifest | Platform team creates before first deploy |
| AzureSqlMI Linked Service (target/RDS) | AWS Glue JDBC Connection → RDS SQL Server | Parser extracts name → app config maps to target role → prereqs manifest | Platform team creates before first deploy |
| SSIS OLEDB Connection Manager | Mapped to above two Glue Connections by name in `connection_mapping` | `connection_classifier.py` maps name to source or target role | Same two Glue Connections cover all OLEDB sources |
| SFTP Linked Service | AWS Transfer Family OR external SFTP (hostname in secret) | `[SFTP_DEPENDENCY]` flagged in FSD. Prereqs manifest: MANUAL_SETUP. | Platform team — cannot be automated |
| HTTP/REST Linked Service | No Glue Connection — Python `requests` library inline | Parser extracts URL → FSD §4 as REST_CALL. VPC egress flagged. | Network team confirms egress rule |
| AzureKeyVault Linked Service | AWS Secrets Manager | Parser maps each reference to Secrets Manager path per app config | Platform team creates secrets |
| SMTP / Send Mail Task | Amazon SES or SNS (redesign required) | Flagged UNSUPPORTED → REDESIGN_REQUIRED | ETL developer rebuilds |
| Linked Server (cross-database) | No direct RDS SQL Server equivalent | Flagged UNSUPPORTED → REDESIGN_REQUIRED | Architecture decision required |

### 6.2 ADF Datasets → Inlined into Script

ADF Datasets do not have a direct AWS equivalent. The parser resolves the full chain:

```
pipeline activity → dataset → linkedService → actual table/path/query
```

The generated script works against concrete table names and connection objects — not abstract dataset references. If a dataset uses parameterised table names, the parser flags `[MANUAL: CONFIRM TABLE NAME — parameter: {param_name}]`.

### 6.3 ADF/SSIS Triggers → EventBridge Rules

| Source trigger type | AWS equivalent | Tool action | Cron note |
|---|---|---|---|
| ADF Schedule Trigger | EventBridge Scheduled Rule | Deployer creates automatically via boto3 | `"0 2 * * *"` → `"cron(0 2 * * ? *)"` — tool translates |
| ADF Tumbling Window | EventBridge Rule + Glue job bookmark | Deployer creates rule; bookmark config added | Same cron translation |
| ADF Blob Event Trigger | S3 Event Notification → EventBridge Rule | SEMI_AUTOMATED — tool generates config, platform applies | Not applicable |
| SSIS SQL Agent Job | EventBridge Scheduled Rule | Parser extracts schedule if in metadata; else `[MANUAL]` | Manual translation if not in dtsx |

### 6.4 Glue Crawlers — When Needed

| Job type | Crawler needed? | Reason |
|---|---|---|
| Python Shell → RDS SQL Server | **NO** | JDBC reads schema from SQL Server directly. No catalog dependency. |
| PySpark → RDS via JDBC | **NO** | Same — JDBC connection, no catalog needed. |
| Visual ETL | YES (S3 sources/targets only) | Visual ETL reads schema from Glue Data Catalog. |
| Job writing to S3 (cold archive, SFTP landing) | **YES** | Athena queries require catalog registration. Auto-created by deployer. |

### 6.5 Secrets Manager — Naming Convention

```
secret/{app_code}/source-azure-sql-mi
secret/{app_code}/target-rds-sql-server
secret/{app_code}/sftp-credentials
secret/{app_code}/api-keys/{service_name}
```

Secret JSON structure (must match exactly — deployer validates before deploy):

```json
{
  "host":     "crm-rds.xxxxxxxx.eu-west-1.rds.amazonaws.com",
  "port":     1433,
  "username": "etl_service_crm",
  "password": "...",
  "database": "CRM_DB"
}
```

---

## 7. FSD Schema

The FSD is generated by the parser and approved by the SME. Everything downstream derives from it.

```markdown
---
job_name: etl_customer_orders_daily
app_code: crm
source_system: SSIS
source_file: source-packages/crm/ssis/etl_customer_orders.dtsx
glue_target: PYTHON_SHELL          # PYTHON_SHELL | PYSPARK | VISUAL_ETL
pattern: THREE_PHASE_STATIC_LOAD   # THREE_PHASE_STATIC_LOAD | GENERAL_ETL
confidence: 89
manual_item_count: 2
python_version: "3.9"
max_capacity: 1.0
glue_iam_role: arn:aws:iam::123456789:role/GlueRole-CRM
vpc_connection_name: glue-vpc-crm
s3_script_path: s3://glue-scripts-prod/crm/etl_customer_orders_daily.py
sns_alert_topic: arn:aws:sns:eu-west-1:123456789:etl-alerts-crm
approved: false
approved_by: ""
approved_date: ""
---

## 1. Business Purpose
[MANUAL INPUT REQUIRED — describe what this job does in business terms]

## 2. Data Sources
| Source ID | Type | Connection role | Table / Query | Row estimate |
|---|---|---|---|---|
| src_orders | SQL Server | SOURCE (Azure SQL MI) | dbo.Orders WHERE order_date >= @load_date | ~2M/day |
| src_products | SQL Server | SOURCE (Azure SQL MI) | dbo.Products | ~50K static |

## 3. Data Targets
| Target ID | Type | Connection role | Schema.Table | Write mode | Merge key |
|---|---|---|---|---|---|
| tgt_staging | SQL Server | TARGET (RDS) | staging.tmp_orders | truncate_insert | — |
| tgt_main | SQL Server | TARGET (RDS) | dbo.Orders_Hist | merge | order_id, order_date |

## 4. Transformations (execution order)
1. TRUNCATE TABLE staging.tmp_orders (DDL — autocommit=True)
2. Extract dbo.Orders filtered to load_date → staging.tmp_orders
3. Lookup dbo.Products on product_id → enrich with product_name, category
4. Derived column: order_value = quantity * unit_price

## 5. SP Dependency Graph
Level 1 (parallel): dbo.sp_validate_orders | dbo.sp_load_reference_data
Level 2 (after Level 1): dbo.sp_enrich_orders | dbo.sp_calculate_pricing
Level 3 (after Level 2): dbo.sp_finalise_orders
SP execution target: TARGET (RDS SQL Server)

## 6. Control Flow
Pattern: THREE_PHASE_STATIC_LOAD
Phase 1: Extract (src_orders + src_products → tgt_staging)
Phase 2: SP execution (see §5 dependency graph)
Phase 3: Transfer (tgt_staging → tgt_main MERGE)

## 7. Error Handling
On failure: preserve staging tables (do not truncate on exception).
SNS alert to: arn:aws:sns:eu-west-1:123456789:etl-alerts-crm
Retry: 1 attempt after 5 minutes.

## 8. Schedule & Triggers
ADF trigger type: ScheduleTrigger
ADF cron: "0 2 * * *"
AWS EventBridge cron: "cron(0 2 * * ? *)"
[MANUAL: confirm no upstream pipeline dependency before this job runs]

## 9. Parameters & Variables
| Name | Type | Default | Classification | Generated as |
|---|---|---|---|---|
| load_date | String | current date | JOB_PARAMETER | getResolvedOptions |
| batch_id | Int | 0 | JOB_PARAMETER | getResolvedOptions |

## 10. Infrastructure Dependencies
- Glue Connection: glue-conn-crm-source-azure-sql (must exist)
- Glue Connection: glue-conn-crm-target-rds (must exist)
- Secret: secret/crm/source-azure-sql-mi (must exist)
- Secret: secret/crm/target-rds-sql-server (must exist)
- Crawler: NOT required (SQL Server target, no S3 writes)

## 11. Technical Constraints
- chunksize=50000 for all pandas read_sql calls
- autocommit=True for ALL DDL (TRUNCATE, DROP, CREATE, sp_rename)
- autocommit=False for ALL DML (INSERT, UPDATE, MERGE, DELETE)
- Drop staging.tmp_orders_new at script start if exists (idempotency)
- VPC connection name: glue-vpc-crm

## 12. MANUAL Items Requiring SME Input Before Approval
- [ ] §1: Business purpose description
- [ ] §8: Confirm no upstream pipeline dependency
```

---

## 8. GHCP Prompt Structure

### 8.1 Prompt Template

The tool generates one prompt file per approved FSD (`tool.py prompt`). Developer opens in VS Code and pastes into GHCP chat panel.

```
# GHCP Generation Prompt — etl_customer_orders_daily.py
# App: CRM | Target: PYTHON_SHELL | Pattern: THREE_PHASE_STATIC_LOAD
# Paste this entire file into GHCP chat

## YOUR TASK
Generate a COMPLETE, PRODUCTION-READY AWS Glue Python Shell script.
Output ONLY the Python script. No explanation. No markdown code fences.
The output will be saved directly as etl_customer_orders_daily.py

## STRICT ARCHITECTURAL RULES — DO NOT DEVIATE

### Job type
AWS Glue PYTHON SHELL job. NOT PySpark.
No SparkContext. No GlueContext. No awsglue imports.
Single-node Python 3.9 container.

### Database connections — TWO SERVERS (IN_MIGRATION stage)
SOURCE: Azure SQL MI (read-only during migration)
  secret_name: secret/crm/source-azure-sql-mi
  Use: sqlalchemy create_engine (mssql+pymssql) for pandas read_sql

TARGET: AWS RDS SQL Server (all writes)
  secret_name: secret/crm/target-rds-sql-server
  Use: sqlalchemy create_engine for DataFrame writes
  Use: raw pymssql connection (autocommit=True) for DDL only
  Use: raw pymssql connection (autocommit=False) for DML transactions

STORED PROCEDURES: execute on TARGET (RDS SQL Server)
  Use raw pymssql connection (autocommit=False) for all EXEC sp_* calls

NEVER mix source and target connections.
NEVER hardcode server names, usernames, passwords, or S3 paths.

### Required libraries
import sys, boto3, json, logging, time
import pandas as pd
from sqlalchemy import create_engine
import pymssql
from awsglue.utils import getResolvedOptions

### Memory constraint
ALL pandas.read_sql calls MUST include chunksize=50000.
Process data in chunks. Never load entire table into memory.

### DDL rule (CRITICAL)
autocommit=True BEFORE any DDL (TRUNCATE, DROP, CREATE, sp_rename).
autocommit=False BEFORE any DML (INSERT, UPDATE, MERGE, DELETE).
Failing to set autocommit=True before DDL causes SQL Server transaction deadlocks.

### Idempotency rule
At script start: DROP TABLE IF EXISTS staging.tmp_orders_new
Ensures the script can be re-run safely if it fails halfway through.

### Error handling
Wrap all database operations in try/except.
On exception: DO NOT truncate staging tables (preserve for diagnosis).
Send SNS alert: {SNS_TOPIC_ARN}
Re-raise the exception after alerting.

## BASE TEMPLATE TO FOLLOW
[tool inserts python_shell_base.py content here]

## JOB SPECIFICATION
[tool inserts full FSD content here — all sections §1 through §12]

## THREE-PHASE EXECUTION ORDER
# PHASE 1: Extract from SOURCE → write to TARGET staging
#   1a. DROP TABLE IF EXISTS staging.tmp_orders_new (DDL conn)
#   1b. TRUNCATE TABLE staging.tmp_orders (DDL conn)
#   1c. Read dbo.Orders chunks from SOURCE → write to staging.tmp_orders (TARGET)
#   1d. Read dbo.Products from SOURCE → write to staging.tmp_products (TARGET)

# PHASE 2: SP execution on TARGET (RDS) — follow dependency graph exactly
#   Level 1: EXEC dbo.sp_validate_orders | EXEC dbo.sp_load_reference_data
#   Level 2: EXEC dbo.sp_enrich_orders | EXEC dbo.sp_calculate_pricing
#   Level 3: EXEC dbo.sp_finalise_orders

# PHASE 3: Transfer from TARGET staging → TARGET main
#   MERGE dbo.Orders_Hist USING staging.tmp_orders ON order_id, order_date
#   Single pymssql transaction (autocommit=False), COMMIT after MERGE

## GENERATE THE COMPLETE SCRIPT NOW.
```

### 8.2 Base Template (python_shell_base.py)

```python
#!/usr/bin/env python3
"""
AWS Glue Python Shell Job — {APP_CODE} | {JOB_NAME}
Generated by: Migration Accelerator + GHCP | Stage: {MIGRATION_STAGE}
ARCHITECTURE: Python 3.9 | pandas + sqlalchemy (reads) | pymssql (DDL + DML)
DO NOT hardcode credentials, bucket names, or server addresses.
"""
import sys, boto3, json, logging, time
import pandas as pd
from sqlalchemy import create_engine
import pymssql
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# ── Constants ─────────────────────────────────────────────────
REGION         = "{AWS_REGION}"
SOURCE_SECRET  = "{SOURCE_SECRET_NAME}"
TARGET_SECRET  = "{TARGET_SECRET_NAME}"
SOURCE_DB      = "{SOURCE_DATABASE}"
TARGET_DB      = "{TARGET_DATABASE}"
STAGING_SCHEMA = "{STAGING_SCHEMA}"
SNS_TOPIC_ARN  = "{SNS_TOPIC_ARN}"
JOB_NAME       = args["JOB_NAME"]
CHUNKSIZE      = 50000    # NEVER change — memory constraint (1 DPU = 16GB)

# ── Secrets Manager ───────────────────────────────────────────
def get_secret(secret_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name=REGION)
    return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])

# ── Connection helpers ─────────────────────────────────────────
def get_source_engine():
    """sqlalchemy engine — Azure SQL MI source (reads only)"""
    s = get_secret(SOURCE_SECRET)
    return create_engine(f"mssql+pymssql://{s['username']}:{s['password']}@{s['host']}/{SOURCE_DB}")

def get_target_engine():
    """sqlalchemy engine — RDS SQL Server (pandas DataFrame writes)"""
    s = get_secret(TARGET_SECRET)
    return create_engine(f"mssql+pymssql://{s['username']}:{s['password']}@{s['host']}/{TARGET_DB}")

def get_ddl_conn():
    """Raw pymssql — DDL only. autocommit=True is MANDATORY for DDL."""
    s = get_secret(TARGET_SECRET)
    conn = pymssql.connect(server=s["host"], user=s["username"],
                           password=s["password"], database=TARGET_DB)
    conn.autocommit(True)
    return conn

def get_dml_conn():
    """Raw pymssql — DML and SP calls. autocommit=False for explicit transaction."""
    s = get_secret(TARGET_SECRET)
    conn = pymssql.connect(server=s["host"], user=s["username"],
                           password=s["password"], database=TARGET_DB)
    conn.autocommit(False)
    return conn

# ── SNS alerting ──────────────────────────────────────────────
def send_alert(message: str):
    try:
        boto3.client("sns", region_name=REGION).publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"ETL FAILURE: {JOB_NAME}",
            Message=message
        )
    except Exception as e:
        log.error(f"SNS alert failed: {e}")

# ── Main ──────────────────────────────────────────────────────
def main():
    log.info(f"Starting {JOB_NAME}")
    # GHCP fills in Phase 1, Phase 2, Phase 3 here

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        send_alert(str(e))
        raise
```

---

## 9. Prerequisites Manifest

`python tool.py prereqs --app crm` outputs this. The deployer validates every item before deploying any job.

```
═══════════════════════════════════════════════════════════════
PREREQUISITES MANIFEST — CRM Application (112 jobs)
Generated: 2026-06-23
═══════════════════════════════════════════════════════════════

SECRETS MANAGER  [create before ANY deployment]
  □ secret/crm/source-azure-sql-mi
  □ secret/crm/target-rds-sql-server
  □ secret/crm/sftp-credentials          [3 jobs depend on this]

GLUE CONNECTIONS  [create before ANY deployment]
  □ glue-conn-crm-source-azure-sql       [JDBC | VPC subnet required]
  □ glue-conn-crm-target-rds             [JDBC | VPC subnet required]

IAM ROLES  [confirm exist with required policies]
  □ GlueRole-CRM  [S3, secretsmanager, glue, logs, sns permissions]

S3 BUCKETS  [confirm exist]
  □ s3://glue-scripts-prod/crm/
  □ s3://data-lake-prod/crm/             [8 jobs write to this]

NETWORK
  □ VPC peering / PrivateLink: Glue → Azure SQL MI
  □ VPC peering / PrivateLink: Glue → RDS SQL Server
  □ Security group: Glue workers → RDS port 1433 inbound

AUTO-CREATED BY DEPLOYER  [no action needed]
  ○ EventBridge rules (104 schedule triggers)
  ○ Glue crawlers (8 S3-writing jobs)
  ○ Glue Data Catalog databases

MANUAL SETUP REQUIRED
  ⚠ AWS Transfer Family SFTP endpoint    [3 jobs]
  ⚠ S3 event notification config         [2 blob-event-trigger jobs]

REDESIGN REQUIRED
  ✗ SSIS Send Mail Task (2 jobs)         → rebuild as SES/SNS
  ✗ Linked Server call (1 job)           → no RDS equivalent
  ✗ MSDTC distributed transaction (1 job) → not supported on RDS

═══════════════════════════════════════════════════════════════
STATUS: NOT READY FOR DEPLOYMENT
Blockers: 3 secrets | 2 Glue connections | VPC peering unconfirmed
═══════════════════════════════════════════════════════════════
```

---

## 10. QA Checks

| Rule ID | Check | Severity | What it catches |
|---|---|---|---|
| QA-01 | No hardcoded connection strings or passwords | **BLOCK** | Any `server=`, `Data Source=`, `password=` patterns outside `get_secret()` |
| QA-02 | No hardcoded S3 bucket names | **BLOCK** | Any `s3://` literal not referencing a parameter or constant |
| QA-03 | chunksize present in all read_sql calls | **BLOCK** | `pandas.read_sql()` without `chunksize=` — memory risk on 1 DPU |
| QA-04 | autocommit=True before DDL | **BLOCK** | TRUNCATE/DROP/CREATE/sp_rename without preceding `autocommit(True)` |
| QA-05 | autocommit=False before DML transactions | WARNING | INSERT/UPDATE/MERGE/DELETE without explicit `autocommit(False)` |
| QA-06 | SNS alert present in except block | WARNING | No `send_alert()` call in the main except block |
| QA-07 | Idempotency — DROP IF EXISTS at start | WARNING | No `DROP TABLE IF EXISTS` for staging tables at script start |
| QA-08 | No PySpark imports in Python Shell job | **BLOCK** | `import pyspark` / `from awsglue.context import GlueContext` in PYTHON_SHELL |
| QA-09 | Python 3.9 syntax compatibility | **BLOCK** | `match/case` (3.10+), union type hints `X\|Y`, walrus without compat |
| QA-10 | Source connection used for reads only | WARNING | `source_engine` used in a write/to_sql/insert context |
| QA-11 | getResolvedOptions present | WARNING | `sys.argv` parsed manually instead of `getResolvedOptions` |
| QA-12 | No bare except clauses | WARNING | `except:` without exception type |

---

## 11. Deployment

```bash
$ python tool.py deploy --app crm --job etl_customer_orders_daily

Pre-flight checks...
  ✅ FSD found and approved (J.Smith, 2026-06-20)
  ✅ Secret: secret/crm/source-azure-sql-mi  EXISTS
  ✅ Secret: secret/crm/target-rds-sql-server  EXISTS
  ✅ Glue connection: glue-conn-crm-source-azure-sql  EXISTS
  ✅ Glue connection: glue-conn-crm-target-rds  EXISTS

QA checks...
  ✅ QA-01 through QA-04, QA-08, QA-09: PASS
  ⚠  QA-06: SNS alert missing from inner SP except block (line 87) — WARNING

Deploying...
  ✅ Uploaded to s3://glue-scripts-prod/crm/etl_customer_orders_daily.py
  ✅ Glue job created: etl_customer_orders_daily
     WorkerType: pythonshell | PythonVersion: 3.9 | MaxCapacity: 1.0
  ✅ EventBridge rule created: cron(0 2 * * ? *)
  ✅ Script archived to deployed/crm/etl_customer_orders_daily.py

Deployment complete. QA warnings: 1 (see reports/crm_qa_report.txt)
```

**Batch deploy:**

```bash
python tool.py deploy --app crm --all              # all approved jobs
python tool.py deploy --app crm --all --dry-run    # validate only, no AWS calls
python tool.py deploy --app crm --all --target-type PYTHON_SHELL
```

---

## 12. Programme Dashboard

```bash
$ python tool.py dashboard

App          Total  Parsed  FSD_Appr  Generated  QA_Pass  Deployed  Progress
─────────────────────────────────────────────────────────────────────────────
CRM            112     112        98         87       85        72      64%
Finance         94      94        60         40       38        20      21%
Logistics      131       0         0          0        0         0       0%
HR              87      87        87         80       79        79      91%
Procurement     76      76        70         65       63        60      79%
... (9 more apps)
─────────────────────────────────────────────────────────────────────────────
TOTAL         1456     812       621        495      480       412      28%

CLASSIFICATION:  PYTHON_SHELL: 389 (48%)  PYSPARK: 298 (37%)  VISUAL_ETL: 125 (15%)

BLOCKERS:
  Logistics: VPC peering pending (network team)
  Finance:   18 jobs awaiting SME FSD approval (SME on leave until Jun 30)
```

---

## 13. Sprint Build Plan

| Sprint | Duration | Deliverable | Acceptance Criteria |
|---|---|---|---|
| **Sprint 0 — Skeleton** | Week 1 | `tool.py` CLI scaffold \| app config loader \| directory structure \| base Python Shell template \| manual deploy | `tool.py parse --app crm` runs without error. `tool.py deploy --app crm --job test` uploads script to S3 and creates a runnable Glue job. |
| **Sprint 1 — ADF Copy Parser** | Week 2 | `adf_parser.py`: Copy Activity + SP Activity \| dataset resolution \| FSD writer \| `connection_classifier.py` | Parse 10 real ADF pipelines. FSD §2, §3, §4 populated correctly. Unknown connections flagged `[MANUAL]`. |
| **Sprint 2 — SSIS Parser** | Weeks 2–3 | `ssis_parser.py`: OLEDB Source/Dest, Execute SQL Task, Data Flow (standard components) \| three-phase pattern detector \| SP dependency graph | Parse 10 real SSIS packages. Three-phase pattern correctly detected. SP dependency graph matches manually traced graph. |
| **Sprint 3 — GHCP Prompt Generator** | Week 3 | `prompt_generator.py`: builds structured GHCP prompt from approved FSD \| base template injection \| three-phase execution order section | Paste generated prompt into GHCP → GHCP generates script passing all QA checks first attempt for > 70% of test jobs. |
| **Sprint 4 — QA + Deploy Automation** | Week 4 | `qa_checker.py`: all 12 QA rules \| `glue_deployer.py` \| `eventbridge_deployer.py` \| `prereqs_checker.py` | Deploy 5 test jobs end-to-end. All QA checks enforce correctly. Glue jobs appear in AWS console and run successfully. |
| **Sprint 5 — ADF Data Flow Parser** | Week 5 | `adf_parser.py` extended: Mapping Data Flow transform nodes \| PYSPARK classification \| PySpark base template \| ForEach/IfCondition structure capture | Parse 5 real ADF Data Flow pipelines. PYSPARK classification correct. PySpark job runs on Glue. |
| **Sprint 6 — Census + Dashboard** | Weeks 5–6 | `census` command \| `prereqs` command \| `dashboard` command | Run census on all 14 applications. Prereqs manifest accurate. Dashboard shows correct counts. |
| **Sprint 7 — Calibration** | Weeks 6–7 | Run tool on first 200 real packages across 3 applications. Tune parser heuristics. | MANUAL flag rate < 30%. Confidence > 75 for > 70% of jobs. GHCP first-attempt success rate > 75% for Python Shell jobs. ETL Lead accepts tool for production use. |

---

## 14. Developer Implementation Sessions

> **How to use this section**
>
> Each session is one focused VS Code / GHCP conversation. Open a new GHCP chat for each session.  
> Open the listed files in VS Code **before** starting — GHCP works best with file context.  
> Paste the starter prompt exactly as shown, then follow up conversationally.  
> **Do not start the next session until the acceptance test passes.**

---

### Session 1 — Project scaffold and app config loader
**Sprint: Sprint 0**

**Open in VS Code before starting:**
- `config/apps/app_crm.yaml` (create from §3.1 schema)
- `tool/__init__.py`

**Paste this into GHCP chat:**

```
I am building an ETL Migration Accelerator tool in Python. It is a VS Code workspace CLI
that migrates SSIS and ADF packages to AWS Glue jobs. 14 applications, 100+ jobs each.

Create the following files:

1. tool/cli.py — argparse CLI with six commands: parse, census, prereqs, prompt, deploy,
   dashboard. Each command takes --app as required argument and --job as optional.
   Print "command --app {app} not yet implemented" as placeholder for each.

2. tool/config_loader.py — loads config/apps/app_{code}.yaml into a typed AppConfig
   dataclass. Fields match this schema:
   app_name, app_code, migration_stage (str), source (secret_name, database, schema),
   target (secret_name, database, staging_schema), sp_execution_target, aws_region,
   glue_iam_role, vpc_connection_name, s3_scripts_bucket, sns_alert_topic,
   connection_mapping (dict[str, str]), python_shell (python_version, max_capacity float,
   chunksize int, additional_python_modules str).

3. tool/__init__.py — empty init.

Use Python 3.9 compatible syntax throughout. Use pyyaml for YAML loading. Full type hints.
```

**Acceptance test:**
```bash
python tool.py parse --app crm        # prints "parse --app crm not yet implemented"
python tool.py deploy --app crm --job test  # prints deploy placeholder
# Config loads without error when crm.yaml exists
```

**Files produced:** `tool/cli.py` | `tool/__init__.py` | `tool/config_loader.py`

---

### Session 2 — Base Python Shell template and manual deploy
**Sprint: Sprint 0**

**Open in VS Code before starting:**
- `config/apps/app_crm.yaml`
- `tool/config_loader.py`

**Paste this into GHCP chat:**

```
I have a Migration Accelerator tool for AWS Glue. Create two things:

1. config/base_templates/python_shell_base.py — complete AWS Glue Python Shell base
   template with:
   - Python 3.9 only. No PySpark. No SparkContext.
   - getResolvedOptions for JOB_NAME parameter
   - CONSTANTS block: REGION, SOURCE_SECRET, TARGET_SECRET, SOURCE_DB, TARGET_DB,
     STAGING_SCHEMA, SNS_TOPIC_ARN, JOB_NAME, CHUNKSIZE = 50000
   - get_secret() using boto3 secretsmanager
   - get_source_engine() → sqlalchemy mssql+pymssql → SOURCE_SECRET (Azure SQL MI)
   - get_target_engine() → sqlalchemy mssql+pymssql → TARGET_SECRET (RDS SQL Server)
   - get_ddl_conn() → raw pymssql, autocommit(True) — for TRUNCATE/DROP/CREATE/sp_rename
   - get_dml_conn() → raw pymssql, autocommit(False) — for INSERT/UPDATE/MERGE/SP EXEC
   - send_alert() → boto3 SNS publish
   - main() stub with try/except calling send_alert and re-raising
   Use {PLACEHOLDER} strings for all app-specific values.

2. tool/deployer/glue_deployer.py — deploys a .py script to AWS Glue:
   - upload_script(local_path, s3_bucket, job_name) → uploads to S3 via boto3
   - create_or_update_job(job_name, s3_path, app_config: AppConfig, fsd_config: dict)
     Uses glue.create_job() or glue.update_job() as appropriate.
     Sets: Command.Name=pythonshell, Command.PythonVersion=3.9, MaxCapacity=1.0,
     Role=app_config.glue_iam_role, Connections=[app_config.vpc_connection_target],
     DefaultArguments: --additional-python-modules, --connection-name
   - create_eventbridge_rule(job_name, cron_aws, app_config) → boto3 events client
```

**Acceptance test:**
Manually create a simple test script using the base template. Run `python tool.py deploy --app crm --job test_job`. Glue job appears in AWS console and runs with exit code 0.

**Files produced:** `config/base_templates/python_shell_base.py` | `tool/deployer/glue_deployer.py`

---

### Session 3 — ADF JSON parser — Copy Activity and Stored Procedure Activity
**Sprint: Sprint 1**

**Open in VS Code before starting:**
- `source-packages/crm/adf/` (real ADF export folder — pipeline/, dataset/, linkedService/)
- `tool/config_loader.py`

**Paste this into GHCP chat:**

```
Build tool/parser/adf_parser.py and tool/parser/connection_classifier.py.

ADF exports have three subfolders: pipeline/, dataset/, linkedService/.

ParsedJob dataclass fields: job_name, sources (list[DataSource]), targets (list[DataTarget]),
activities (list[ParsedActivity]), parameters (list[JobParameter]),
connection_refs (list[ConnectionRef]), has_stored_procedure_calls (bool),
sp_calls (list[SpCall]), sp_dependency_graph (dict[str, list[str]]).

AdfParser.parse_pipeline(pipeline_json_path: str, app_config: AppConfig) -> ParsedJob

Handle ONLY these activity types:
- Copy (typeProperties.source + .sink): resolve dataset reference → read dataset JSON →
  resolve linkedService name → extract table name and connection type
- SqlServerStoredProcedure (typeProperties.storedProcedureName + .storedProcedureParameters)
- ExecutePipeline: flag as nested dependency in activities list

Dataset resolution chain:
  activity.inputs[0].referenceName
  → read dataset/{name}.json
  → extract typeProperties.tableName (or schema + table)
  → read linkedService/{linkedServiceName}.json
  → extract connection type (AzureSqlMI | SqlServer | etc.)

ConnectionClassifier.classify(linked_service_name: str, app_config: AppConfig)
  → looks up name in app_config.connection_mapping
  → returns ConnectionRole: SOURCE | TARGET | SFTP | UNKNOWN
  → UNKNOWN → flag as [MANUAL: UNKNOWN CONNECTION]

Use Python 3.9 compatible syntax. Full type hints.
Write tests/test_adf_parser.py with a minimal inline fixture ADF JSON.
```

**Acceptance test:**
`python tool.py parse --app crm` produces `fsd/crm/{job_name}.md` for each ADF Copy + SP pipeline. §2 sources, §3 targets, §4 SP calls populated. Unknown connections flagged `[MANUAL]`.

**Files produced:** `tool/parser/adf_parser.py` | `tool/parser/connection_classifier.py` | `tests/test_adf_parser.py`

---

### Session 4 — FSD writer — Markdown output from ParsedJob
**Sprint: Sprint 1**

**Open in VS Code before starting:**
- `tool/parser/adf_parser.py`
- The FSD schema from §7 of this document (open this file)

**Paste this into GHCP chat:**

```
Build tool/fsd/fsd_writer.py.

FsdWriter.write(parsed_job: ParsedJob, app_config: AppConfig, output_dir: str) -> str
(returns path to written FSD file)

Rules:
- Front-matter: populate all fields from parsed_job and app_config.
  Set approved: false, approved_by: "", approved_date: "".
- §1 Business Purpose: always write [MANUAL INPUT REQUIRED — describe what this job
  does in business terms]
- §2 Data Sources: one table row per source. If ConnectionRole is UNKNOWN, write:
  SOURCE (⚠ UNKNOWN — classify in app config connection_mapping)
- §4 Transformations: numbered list in execution order from parsed_job.activities
- §5 SP Dependency Graph: if sp_dependency_graph populated, write level structure.
  If empty but has_stored_procedure_calls is True, write:
  [MANUAL: SP ORDER UNKNOWN — list SPs and their execution dependencies]
- §8 Schedule & Triggers: if no trigger found, write:
  [MANUAL: confirm schedule — no trigger found in source package]
- §12 MANUAL Items: auto-generate checklist by scanning entire document for [MANUAL]
  occurrences. Write as unchecked checkboxes.
- Count [MANUAL] occurrences → write manual_item_count to front-matter.

Write tests/test_fsd_writer.py.
```

**Acceptance test:**
FSD files are valid Markdown. `manual_item_count` in front-matter matches actual `[MANUAL]` count in document body. SME can open FSD in VS Code, fill items, and set `approved: true`.

**Files produced:** `tool/fsd/fsd_writer.py` | `tests/test_fsd_writer.py`

---

### Session 5 — SSIS .dtsx XML parser
**Sprint: Sprint 2**

**Open in VS Code before starting:**
- `source-packages/crm/ssis/` (real .dtsx files)
- `tool/parser/adf_parser.py` (reference for ParsedJob shape)
- `tool/parser/connection_classifier.py`

**Paste this into GHCP chat:**

```
Build tool/parser/ssis_parser.py.

SsisParser.parse_package(dtsx_path: str, app_config: AppConfig) -> ParsedJob

Also read companion files if they exist in the same directory:
  {package_name}.conmgr → connection manager definitions
  {package_name}.params → package parameter definitions

Extract from .dtsx XML:
1. DTS:ConnectionManager — name + ConnectionString → parse server and database →
   pass through connection_classifier.classify()
2. DTS:Parameter — name, DataType, DefaultValue → classify as JOB_PARAMETER or CONSTANT
3. DTS:PackageVariable — name, DataType, Value
4. Data Flow Tasks (DTS:Executable[@ExecutableType="Microsoft.Pipeline"]):
   - DTSAdapter.OleDbSource → source_read (SqlCommand or table name)
   - DTSAdapter.OleDbDestination → target_write (table name)
   - DTSTransform.Lookup → join (connection ref, input/output columns, no-match disposition)
   - DTSTransform.DerivedColumn → derive (output column name + expression string)
   - DTSTransform.ConditionalSplit → filter (conditions in priority order)
   - Microsoft.SqlServer.Dts.Tasks.ScriptTask → flag [MANUAL: SCRIPT COMPONENT — paste
     C# body into GHCP separately for translation]
5. Execute SQL Tasks — extract connection name + SQL statement.
   If SQL starts with EXEC or EXECUTE → classify as sp_call.
6. Precedence constraints — extract from/to task names → adjacency list →
   pass to pattern_detector.detect()

Build tool/parser/pattern_detector.py:
  detect(adjacency_list, activities) → sets ParsedJob.pattern (THREE_PHASE_STATIC_LOAD
  or GENERAL_ETL) and ParsedJob.sp_dependency_graph (dict[str, list[str]])

Detection signals:
  Phase 1: TRUNCATE in Execute SQL at package start + OleDbDestination to staging table
  Phase 2: Execute SQL Tasks calling SPs
  Phase 3: Execute SQL with INSERT INTO main SELECT * FROM staging, or MERGE, or sp_rename

Write tests/test_ssis_parser.py with tests/fixtures/sample.dtsx as a minimal valid dtsx.
```

**Acceptance test:**
Parse 10 real SSIS packages. Three-phase pattern correctly detected. SP dependency graph matches manually traced graph. Script Components flagged `[MANUAL]`.

**Files produced:** `tool/parser/ssis_parser.py` | `tool/parser/pattern_detector.py` | `tests/test_ssis_parser.py`

---

### Session 6 — GHCP prompt generator
**Sprint: Sprint 3**

**Open in VS Code before starting:**
- `fsd/crm/etl_orders_daily.md` (an approved FSD with `approved: true`)
- `config/base_templates/python_shell_base.py`
- The GHCP prompt template from §8.1 of this document

**Paste this into GHCP chat:**

```
Build tool/prompt/prompt_generator.py.

PromptGenerator.generate(fsd_path: str, app_config: AppConfig) -> str

Steps:
1. Read FSD. Parse YAML front-matter (pyyaml).
2. Check approved: true — if false, raise ValueError("FSD not approved. Set approved: true
   after SME review before generating prompt.")
3. Check manual_item_count == 0 — if > 0, raise ValueError(f"{n} [MANUAL] items remain.
   Resolve all before generating prompt.")
4. Build prompt string following the template in §8.1 of the implementation doc:
   - Task section
   - Strict architectural rules section
   - Connection rules section — if migration_stage == IN_MIGRATION: dual connection pattern.
     If POST_CUTOVER: single connection (both source and target use TARGET secret).
   - Required libraries section
   - Memory constraint, DDL rule, idempotency rule, error handling sections
   - Base template content (read from config/base_templates/{glue_target}_base.py)
   - Full FSD content (all sections §1–§12)
   - If pattern == THREE_PHASE_STATIC_LOAD: add Three-Phase Execution Order section
     with SP dependency graph formatted as numbered levels
5. Write prompt to prompts/{app_code}/{job_name}_prompt.md
6. Print: "Prompt written to prompts/{app}/{job}_prompt.md
          Open this file in VS Code and paste the entire contents into your GHCP chat."

Update tool/cli.py to call PromptGenerator when command == "prompt".
```

**Acceptance test:**
`python tool.py prompt --app crm --job etl_orders_daily` → produces prompt file. Paste into GHCP → generated script passes QA-01 through QA-09 without manual correction for a simple Copy + SP pattern job.

**Files produced:** `tool/prompt/prompt_generator.py` | `prompts/` folder

---

### Session 7 — QA checker and full deploy pipeline
**Sprint: Sprint 4**

**Open in VS Code before starting:**
- `generated/crm/etl_orders_daily.py` (a GHCP-generated script to test against)
- `tool/deployer/glue_deployer.py`

**Paste this into GHCP chat:**

```
Build tool/qa/qa_checker.py.

QaChecker.check(script_path: str, fsd_config: dict) -> QaResult
QaResult: passed (bool), blocks (list[QaViolation]), warnings (list[QaViolation])
QaViolation: rule_id (str), description (str), line_number (int | None)

Implement all 12 QA rules from §10 of the implementation doc:
BLOCK rules (QA-01, QA-02, QA-03, QA-04, QA-08, QA-09): passed=False if any present
WARNING rules (QA-05, QA-06, QA-07, QA-10, QA-11, QA-12): passed=True but logged

Implementation:
- QA-01, QA-02, QA-10: string pattern matching (re module)
- QA-03, QA-05, QA-06, QA-07, QA-11, QA-12: Python ast module — walk AST to find calls
- QA-04: find DDL keywords (TRUNCATE, DROP, CREATE, sp_rename) in script text,
  then check the preceding 5 lines for autocommit(True). If not found → BLOCK.
- QA-08: check imports for pyspark, awsglue.context
- QA-09: try compile(script_text, filename, "exec") — SyntaxError → BLOCK

Then update tool/deployer/glue_deployer.py to orchestrate the full deploy sequence:
  1. QaChecker.check() — if any BLOCK → print violations and sys.exit(1)
  2. Check secrets exist (boto3 secretsmanager describe_secret)
  3. Check Glue connections exist (boto3 glue get_connection)
  4. Upload script to S3
  5. Create or update Glue job
  6. Create EventBridge rule (if schedule in FSD front-matter)
  7. Create Glue crawler (if any target in FSD has type S3)
  8. Write QA report to reports/{app_code}_qa_report.txt
  9. Copy script to deployed/{app_code}/{job_name}.py
```

**Acceptance test:**
Insert deliberate violations: hardcoded password → blocks deploy (QA-01). Missing SNS call → warning only, deploy proceeds. End-to-end deploy of a real CRM job creates Glue job + EventBridge rule in AWS.

**Files produced:** `tool/qa/qa_checker.py` | `reports/` folder | `deployed/` folder

---

### Session 8 — Prerequisites manifest and census report
**Sprint: Sprint 4**

**Open in VS Code before starting:**
- `config/apps/` (all app YAML files)
- `reports/` folder (if any existing reports)

**Paste this into GHCP chat:**

```
Build two reporting modules.

1. tool/reporting/census.py — CensusReporter.generate(app_code: str, app_config: AppConfig)

   Reads all source files in source-packages/{app_code}/. For each file, runs the
   appropriate parser (SsisParser or AdfParser based on file extension .dtsx vs .json).
   Outputs reports/{app_code}_census.xlsx with columns:
     job_name | source_file | source_type | glue_target | pattern | sp_count |
     manual_item_count | confidence | unknown_connections | has_script_component |
     has_unsupported | recommended_action | sme_approved (blank)
   
   recommended_action values:
     AUTO_APPROVE (confidence >= 85, manual_items == 0)
     REVIEW_FSD (confidence 70-84 or manual_items 1-2)
     MANUAL_DEVELOPER (confidence < 70, manual_items > 3, or has_unsupported)
   
   Print summary: total | PYTHON_SHELL | PYSPARK | VISUAL_ETL | avg confidence

2. tool/reporting/prereqs.py — PrereqsReporter.generate(app_code, app_config, census_df)

   Scans all parsed jobs. Builds prerequisites manifest (format from §9 of implementation doc).
   For each prerequisite, check against AWS:
     - Secrets: boto3 secretsmanager describe_secret → ✅ EXISTS | □ MISSING
     - Glue Connections: boto3 glue get_connection → ✅ EXISTS | □ MISSING
     - S3 Buckets: boto3 s3 head_bucket → ✅ EXISTS | □ MISSING
   Mark auto-created items as ○ (deployer handles).
   Mark manual/redesign items as ⚠ or ✗.
   Output reports/{app_code}_prereqs.txt.
   Print: READY_FOR_DEPLOYMENT | NOT_READY (with blocker count).
```

**Acceptance test:**
`python tool.py census --app crm` → produces `crm_census.xlsx` with correct classification for all parsed jobs. `python tool.py prereqs --app crm` → `crm_prereqs.txt` with accurate EXISTS/MISSING status.

**Files produced:** `tool/reporting/census.py` | `tool/reporting/prereqs.py`

---

### Session 9 — ADF Mapping Data Flow parser (PySpark path)
**Sprint: Sprint 5**

**Open in VS Code before starting:**
- `source-packages/crm/adf/dataflow/` (real Data Flow JSON files)
- `tool/parser/adf_parser.py`
- `config/base_templates/` (create `pyspark_base.py` here)

**Paste this into GHCP chat:**

```
Extend tool/parser/adf_parser.py to parse ADF Mapping Data Flow JSON files.

When an ExecuteDataFlow activity is encountered, read the referenced Data Flow JSON
from the dataflow/ subfolder.

Parse transformations[] array. For each node extract type, name, and type-specific config.
Map each type to ParsedActivity:
  source → source_read (resolve dataset reference → table/path + connection role)
  sink → target_write (dataset reference + write mode: truncate, upsert, merge)
  filter → filter (condition expression)
  select → column_select (column list)
  aggregate → aggregate (group_by_keys + measures)
  join → join (join keys, join type, both sources)
  lookup → join (flag as lookup — drives PYSPARK classifier)
  sort → sort (columns + direction)
  union → union (sources list)
  derivedColumn → derive (column name + expression)
  conditionalSplit → multiple filter entries (conditions in order)
  flowlet → flag [MANUAL: FLOWLET]

After parsing, run target_classifier:
  join + aggregate + window + conditionalSplit → PYSPARK
  source + filter + sink only (≤ 2 sources) → VISUAL_ETL candidate

Also create config/base_templates/pyspark_base.py:
  AWS Glue PySpark base template with GlueContext, SparkSession,
  get_source_engine() and get_target_engine() using spark.read.jdbc
  (JDBC URL built from secrets, same dual-connection pattern as Python Shell base).
```

**Acceptance test:**
Parse 5 real ADF Mapping Data Flow pipelines. Complex flows classified PYSPARK. Simple flows classified VISUAL_ETL. FSD §4 lists transformation nodes in correct execution order. PySpark job generated and runs on Glue.

**Files produced:** `tool/parser/adf_parser.py` (extended) | `config/base_templates/pyspark_base.py`

---

### Session 10 — Programme dashboard and full end-to-end test
**Sprint: Sprint 6**

**Open in VS Code before starting:**
- `tool/reporting/census.py`
- `tool/reporting/prereqs.py`
- `config/apps/` (all 14 app YAML files)
- `reports/` (all existing per-app reports)

**Paste this into GHCP chat:**

```
Build tool/reporting/dashboard.py.

DashboardReporter.generate() — reads all app configs from config/apps/*.yaml.
For each app, reads reports/{app_code}_census.xlsx if it exists.

Output:
1. Console table (format from §12 of implementation doc):
   App | Total | Parsed | FSD_Approved | Generated | QA_Pass | Deployed | Progress%
   with TOTAL row, CLASSIFICATION BREAKDOWN, and BLOCKERS section.
   An app is a blocker if its prereqs.txt contains "NOT_READY".

2. reports/programme_dashboard.xlsx with same data plus per-job detail sheet.

Counts:
  Parsed = count of .md files in fsd/{app}/
  FSD_Approved = count where approved: true in front-matter
  Generated = count of .py files in generated/{app}/
  QA_Pass = count of .py files in deployed/{app}/ (only deployed if QA passed)
  Deployed = verify existence of Glue job via boto3 glue get_job (catch exceptions)

Then run a full end-to-end test:
Pick the simplest CRM pipeline (Copy Activity only, no SPs, known table names).
Document each step: parse → validate FSD → approve → generate prompt → paste to GHCP
→ save script → deploy → verify Glue job runs against dev RDS instance.
List any issues found and fixes applied. This becomes the calibration baseline.
```

**Acceptance test:**
`python tool.py dashboard` shows accurate counts for all 14 apps. One complete end-to-end job deployed and running in AWS dev Glue environment. ETL Lead accepts tool for Sprint 7 calibration across real estate.

**Files produced:** `tool/reporting/dashboard.py` | `reports/programme_dashboard.xlsx`

---

## 15. Fallback Plan

| Scenario | Fallback action | Tool still used for? |
|---|---|---|
| SSIS Script Component (C#) | Developer copies C# body into GHCP chat separately: *"Translate this C# data transformation logic to Python 3.9. Use pandas and standard library only. No PySpark."* | FSD skeleton (structure captured), deploy (unchanged) |
| Complex ADF expression tool cannot normalise | Developer fills `[MANUAL: COMPLEX EXPRESSION]` in FSD with natural language description. GHCP generates expression from description in Step 4. | FSD (all other sections), prompt generation, deploy |
| Tool parser fails on unknown XML/JSON structure | Developer writes FSD manually using §7 schema template. All other steps unchanged. | Prompt generation, deploy |
| Sprint 0 not complete by Week 3 | Developer writes scripts manually with GHCP using base template directly. ETL Lead runs deploy step manually for each batch. | Deploy step (independent of parser) |
| VISUAL_ETL candidate | ETL developer configures Visual ETL job in AWS Glue console using FSD §2–§4 as the specification. | FSD generation, prereqs manifest, dashboard progress tracking |

---

*Confidential | June 2026 | ETL Migration Accelerator — Implementation Design Document | v3.0*
