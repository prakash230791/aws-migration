# Email: ADF to Glue Conversion Status and Recommendation on Short-Lived Activities

**Subject:** ADF to Glue Conversion: Status Update and Recommendation on Short-Lived Activities

---

Hi Team,

We have converted 180+ ADF pipelines to AWS Glue solutions using the atx transform. One ETL is deployed and validated end to end; [remaining pipelines: status / planned deployment waves].

Post-conversion changes required to run in the AWS environment:

1. Rewired the converted packages to the client network policy (naming standards, tagging).
2. Excluded IAM provisioning from the converted packages.
3. Updated Glue network configuration to allow self-referencing security group rules.
4. Moved secret management outside the ETL deployment.

## Observation: cold start in Glue Python shell

The converted solution runs lookups, stored procedure invocations and information logging as Glue Python shell jobs. These activities do 1 to 2 seconds of actual work, but each run incurs roughly 1 minute of startup and job-run overhead. Because that overhead is paid on every invocation, it multiplies by the number of times the activity appears in a pipeline. For example, a pipeline with 10 such activities adds about 10 minutes of pure overhead. This puts the current SLAs for App 1 and App 2 at risk.

Beyond latency, there are two further concerns:

- **Concurrency:** a Glue job allows one concurrent run by default. Shared logging or lookup jobs called from parallel branches (ForEach, Parallel) will queue or fail unless we raise the limit per job. Lambda scales concurrency automatically.
- **Shared quota:** with 180+ pipelines, these small jobs consume Glue concurrent-run capacity that the heavy PySpark jobs need.

## Recommendation

Use Lambda for short-lived activities, and keep Glue for work that needs it:

- **Lambda:** lookups, information logging, notifications, and stored procedure calls that finish in seconds. Startup is typically well under a few seconds, and Step Functions invokes it natively.
- **Glue Python shell:** long-running stored procedures (10+ minutes). Lambda's 15-minute limit leaves no margin for these.
- **Glue PySpark:** actual data transformation at volume.

As a working rule: a step that completes in under a minute and runs frequently goes to Lambda.

Implementation notes:

- SQL Server connectivity from Lambda should use pymssql (pure-Python wheel, no ODBC driver install), with VPC attachment and the existing secret retrieval pattern.
- Exception logging can be centralized as a Step Functions Catch routed to one shared logging Lambda, instead of per-script error handling.
- If log-write volume is high, RDS Proxy on the logging path will pool the short-lived connections.
- Where a step only writes to AWS services (SNS, DynamoDB), Step Functions direct integrations can replace the Lambda entirely.

## Ask

1. Update the atx transform template so this classification is applied across all 180+ pipelines, rather than patching each one by hand.
2. Pilot on the pipeline already validated end to end, and compare runtime before and after: [measured runtime today] vs [measured runtime with Lambda].
3. Confirm the SLA targets for App 1 and App 2 so we can size the improvement against them.

Happy to walk through the numbers on a call.

Thanks,
Prakash
