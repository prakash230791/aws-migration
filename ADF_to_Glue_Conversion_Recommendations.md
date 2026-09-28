# ADF → AWS Glue Conversion: Recommendations for atx Transform Team

**Purpose:** Consolidated guidance for handling the output of the atx transform tool (ADF payload → CloudFormation with Step Functions + Glue). These are the checkpoints the team should apply during conversion, not defaults to accept as-is.

**Schema:** S.No | Approach | Execution Path | Impact | Comments

---

## 1. IaC & Tooling Standardization

| S.No | Approach | Execution Path | Impact | Comments |
|---|---|---|---|---|
| 1 | Do not adopt the generated CloudFormation as final IaC without an explicit decision | Confirm with team: convert to Terraform (existing project standard) before deployment, or formally scope CFN to ETL resources only | Avoids two parallel IaC toolchains long-term — Terraform is already the standard on the RDS side of this migration | Needs a named decision owner and sign-off before first prod deployment, not a discovery during ops handoff |

## 2. Compute / Job-Type Classification

| S.No | Approach | Execution Path | Impact | Comments |
|---|---|---|---|---|
| 2 | Classify every generated Glue job by workload type before accepting defaults | For each converted activity: real transform/large-volume work → Glue PySpark; short & infrequent control-flow → Glue Python shell; short & high-frequency (many invocations/day) → Lambda | Prevents paying Spark cluster cost and cold-start tax for trivial tasks, and prevents Lambda's 15-min timeout risk on real data work | Build a simple audit sheet: activity name → current job type → recommended job type |
| 3 | Right-size Glue PySpark worker type and DPU count per job — don't accept generator defaults | Benchmark each job against actual table/data volume | Generic sizing means either overpaying or throttled jobs | Align with the T1 (~2TB) / T2 (10–15TB) tiering already used on the RDS side |

## 3. Orchestration & Error Handling

| S.No | Approach | Execution Path | Impact | Comments |
|---|---|---|---|---|
| 4 | Validate the generated Step Functions ASL against the original ADF pipeline, activity by activity | Manually diff `ForEach` / `If Condition` / `Until` / retry-timeout logic — don't trust the auto-mapping as 1:1 | Silent logic drift is the highest-risk failure mode of automated conversion | Prioritize pipelines with complex branching first |
| 5 | Replace per-script `try/except` logging with a centralized Step Functions `Catch` → one shared "LogException" Lambda task | One `Catch` config reused across every Task state in the ASL | Captures failures the automated conversion typically misses (OOM, timeout, unhandled crash outside the script's own try/except) | Ties into the logging architecture in Section 5 |
| 6 | Consider AWS Glue Workflows instead of Step Functions for simple, linear, low-branching pipelines | Evaluate per pipeline, not as a blanket switch | Removes per-transition Step Functions cost where you don't need `Catch`/`Parallel`/`Map` | Optional — only where complexity doesn't justify Step Functions overhead |

## 4. Stored Procedure & Database Connectivity

| S.No | Approach | Execution Path | Impact | Comments |
|---|---|---|---|---|
| 7 | Keep long-running (10+ min) proc calls on Glue Python shell, synchronous, invoked via Step Functions `.sync` integration | Set the Step Functions task timeout above the proc's expected max runtime, with buffer | Lambda's 15-min hard timeout has no margin for a proc already at 10+ min | Do not build async polling infrastructure — not needed at this scale, adds complexity without benefit |
| 8 | Move short (seconds) logging/notification proc calls to Lambda | Reclassify per the audit sheet from item 2 | Avoids Python shell's ~10–30 sec startup plus 1-minute billing floor for a call that takes 2 seconds | |
| 9 | Use `pymssql`, not `pyodbc`, for SQL Server connectivity from Glue Python shell | `--additional-python-modules pymssql` | `pyodbc` requires a system-level ODBC driver install; Python shell has no root access to add one | Same constraint applies to Lambda unless a custom container image bundles the driver |

## 5. Logging & Control-Table Architecture

| S.No | Approach | Execution Path | Impact | Comments |
|---|---|---|---|---|
| 10 | Decide the control-table destination before conversion: RDS SQL Server (same table) vs. DynamoDB | Check: does anything outside the pipeline — a BI report, an audit/compliance process — query the table directly? If yes → keep RDS. If no → DynamoDB is the better long-term fit | Determines connection-pooling needs and whether schema/consumers must stay stable | Needs a decision owner — don't let atx transform default this silently |
| 11 | Centralize the "write log row" logic into one shared module, not duplicated per generated script | Package as a wheel / shared Glue library referenced via `--extra-py-files` or `--additional-python-modules` | One place to change schema or destination later, instead of N generated scripts | |
| 12 | Don't duplicate what CloudWatch Logs / Step Functions execution history already capture for free (timestamps, duration, pass/fail) | Control table should hold only business-specific fields — table name, row count, which proc ran | Reduces redundant writes and table bloat | |
| 13 | If proc-call volume is high, put RDS Proxy in front of RDS for the logging path | Pool/multiplex the short-lived connections from Lambda/Python-shell log calls | RDS instance already carries the DMS CDC task from the parallel SQL MI → RDS migration — avoid connection contention between the two workstreams | |

## 6. Validation Before Cutover

| S.No | Approach | Execution Path | Impact | Comments |
|---|---|---|---|---|
| 14 | Run a schema diff — not just a row-count check — between ADF output and Glue output, especially datetime/decimal precision | Build into the test harness as an automated step | SQL Server → Spark type coercion is a known silent-failure point | |
| 15 | Audit every generated Glue Connection / Secrets Manager mapping | Confirm each Linked Service's credentials are actually wired through, not hardcoded or left as a placeholder | Auto-conversion frequently leaves connection config incomplete | |
| 16 | Parallel-run reconciliation against the existing ADF pipeline output before cutover | Run both systems for at least one full cycle per pipeline tier, compare output | Standard due diligence for any ETL platform migration | Align with the Week-4 Go/No-Go gate model already used on the RDS side, if the timelines overlap |

---

*Open decisions requiring sign-off before conversion proceeds: Item 1 (IaC standard), Item 10 (control-table destination).*
