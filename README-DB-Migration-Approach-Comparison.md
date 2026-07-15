# Azure SQL MI → AWS RDS SQL Server — Migration Approach Comparison

Homogeneous SQL Server-to-SQL Server move — 8 execution paths evaluated across data-transfer, replication, and hybrid strategies.

## One-Time Bulk Transfer Approaches

Best for the initial seed load, or full migration of non-OLTP tiers where a maintenance window is acceptable.

| S.No | Approach | Execution Path | Impact | Comments |
|---|---|---|---|---|
| 1 | Native Backup/Restore (EC2 staging) | BACKUP TO URL (Blob) → EC2 staging → aws s3 cp → rds_restore_database | One-time window: backup+transfer+restore (~hrs for 2TB). Only connection string changes. | Predictable seed path. EC2 needs EBS sized for the .bak file + good throughput. |
| 2 | Native Backup/Restore (On-prem staging) | BACKUP TO URL → download on-prem → upload to S3 → rds_restore_database | Same as above; duration depends on on-prem bandwidth to both clouds. | Good if on-prem link exists. Usually slower than EC2 unless bandwidth is strong. |
| 3 | AWS DataSync (Blob → S3 direct) | DataSync agent → task copies Blob → S3 → rds_restore_database | Same restore-time impact; removes the manual staging hop. | Best for repeated transfers across many DBs/tiers — retries, metrics, scheduling built-in. |
| 4 | rclone (Blob → S3 direct) | Azure Blob + S3 as rclone remotes → rclone copy → rds_restore_database | Similar to DataSync; can skip a full local download if streaming. | Lighter-weight, less common in enterprise shops. Validate streaming at 2TB+ scale first. |

## Replication-Based & Hybrid Approaches

Best for the OLTP core (T1), where minimizing cutover downtime matters most.

| S.No | Approach | Execution Path | Impact | Comments |
|---|---|---|---|---|
| 5 | AWS DMS (Full Load + CDC) | DMS connects to Azure SQL MI (source) + RDS (target); full load copies data, then CDC captures changes | Near-zero downtime — cutover is final CDC catch-up + connection string switch. Needs VPN/Direct Connect. | Primary method for T1. Homogeneous endpoints — no schema/type mapping needed. |
| 6 | ★ **Hybrid: Native Restore (seed) + DMS CDC-only (delta)** | Backup/restore for bulk seed → DMS CDC-only from restore's LSN → monitor lag → cutover | Minimizes bulk transfer time and ongoing downtime together. | **Recommended for T1.** Avoids DMS re-copying data. Needs precise LSN/timestamp alignment. |
| 7 | Transactional Replication (Publisher/Subscriber) | Azure SQL MI as Publisher, RDS as Subscriber — only if supported on the RDS version | Downtime similar to DMS CDC if it works, but support isn't guaranteed. | Fallback option only. Confirm RDS version support before considering. |
| 8 | Log Shipping / Always On | Not applicable in standard form on RDS | N/A — not viable as a primary mechanism. | RDS has no OS/instance-level access for conventional log shipping. DMS covers this instead. |

## Recommended Approach by Data Tier

| Tier | Data Volume | Recommended Approach | Detail |
|---|---|---|---|
| T1 — OLTP Core | ~2 TB | Hybrid: Native Restore + DMS CDC-only | Fast native seed, then near-zero-downtime CDC delta sync until cutover. |
| T2 — Warm History | 10–15 TB | Native Backup/Restore (EC2 or DataSync) | Scheduled maintenance window is acceptable; DataSync if repeating across databases. |
| T3 — Cold Archive | 33–38 TB | Skip RDS — export to S3 | Compliance/audit-only access pattern: S3 + Parquet + Athena instead of RDS. |

## Pre-Migration Checklist

- [ ] Confirm egress bandwidth from Azure (determines transfer time for large `.bak` files)
- [ ] Confirm EC2/on-prem staging disk size can hold the full backup file if using a two-hop method
- [ ] Confirm IAM role on RDS instance has S3 read access for `rds_restore_database`
- [ ] Confirm RDS storage encryption (KMS) enabled at instance creation — cannot be added retroactively
- [ ] Confirm network path (Direct Connect/VPN) is provisioned if DMS will connect directly to Azure SQL MI
- [ ] For hybrid approach: confirm exact LSN/timestamp alignment between backup/restore completion and DMS CDC start point
- [ ] Define cutover gate criteria (e.g. CDC lag < 2 seconds sustained for 72 hours) before starting CDC monitoring
