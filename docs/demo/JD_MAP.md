# JD map: Toyota Senior Engineer, Data Engineering (Job Id 10333980)

Labels: **C** = confirmed (source named, read 2026-11-07), **I** = inferred, **U** = unknown / not shown.
Evidence paths are relative to this repo at `origin/master` 2d22ca2. "Exists" means the file was read, not that it ran in production.

## What you'll be doing

| JD line (verbatim) | Evidence | Label |
|---|---|---|
| Designing, developing, and maintaining scalable data pipelines using Python, Databricks, and Apache Airflow / AWS Step Functions. | Python/PySpark: `jobs/transform.py:161` `run_pipeline`. Databricks: `databricks/pipeline.py:44-76` (bronze/silver/gold materialized views), `databricks.yml` job `zori_weekly`. Airflow: `dags/rental_pipeline_dag.py:64` (download → transform → DQ → DuckDB+dbt). Scale: one public CSV (8,340 ZIPs), not "large volumes". Step Functions: not shown. | Airflow/Databricks/Python **C**; Step Functions **U** (not shown); "scalable" **I** |
| Implementing and optimizing ETL processes to extract, transform, and load data from enterprise sources into product datastores. | Extract: `jobs/io_utils.py:69` `read_raw_csv` (explicit schema). Transform: `jobs/transform.py:37-158`. Load: partitioned Parquet (`io_utils.py:88`), DuckDB loader. Source is a public Zillow CSV, not an enterprise source. The DAG download task is a stub (`dags/rental_pipeline_dag.py:79` TODO). "Optimizing": one commit coalesces files per partition (81d803b), no benchmark. | ETL **C**; enterprise sources **U**; optimizing **I** |
| Applying data quality and validation techniques to ensure accuracy and reliability of datasets. | Local: `jobs/data_quality.py:41,74,97,130,161` (null %, row count, range, uniqueness, `run_quality_checks`), thresholds in `config/pipeline.yaml`. Databricks: `databricks/pipeline.py:53,66,73-75` expectations; mapping table in `docs/databricks-notes.md` "DQ → expectation mapping". dbt `not_null` + singular tests in `dbt/tests/`. | **C** |
| Troubleshooting and resolving data-related issues to ensure smooth project execution. | `docs/databricks-notes.md` "Failure modes" 1-4 (import path, `__file__`, out-of-range rents in 3 NY ZIPs, fingerprint import). Failure 3 was fixed by raising the shared `rent_range`; the notes call the "high-rent ZIPs" explanation inferred, not verified. 3149225 null-key fix in `no_duplicate_keys`. | events **C**; cause of failure 3 **I** |
| Using AI as an assistant across your daily work — pipeline development, code review, testing, debugging, and documentation — ... while holding quality and review standards. | `AGENTS.md` / `CLAUDE.md` in repo; commit 0330ffb "Address Codex review". What was accepted vs rejected from that review is **not yet checked**; I will read the diff before it goes in `STORIES.md`. | files exist **C**; judgment story **U** until diff read |
| Prototyping and experimenting to answer open questions quickly: build the smallest thing that tests the idea, measure what it shows, and share what you learned — including when it does not work. | Databricks runtime added as optional, with `docs/adr/0001-databricks-optional-runtime.md`; failure-modes table records what did not work. | ADR/notes **C**; "prototype" framing **I** |
| Ensuring non-functional requirements — security, performance, scalability, and system integrations — are met through effective design and development. | Reproducibility: `requirements.lock`, CI installs from lock (f806097), ruff pinned (44300cb). Security, performance, scalability: nothing measured or tested in this repo. | CI/lock **C**; security/perf/scale **U** (not shown) |
| Working with Business Product Owners to refine requirements and acceptance criteria. / Spending time with the planners, logistics, and operations teams who use what you build ... / Collaborating with cross-functional teams ... | Not a repo artifact. Solo portfolio project, no stakeholder. | **U**: answer from other experience, not this repo |
| Implementing technical solutions that align with architectural decisions and enterprise standards. | ADR 0001; `CLAUDE.md` code standards (type hints, pure transforms, config in YAML). | ADR **C**; "enterprise standards" **I** |
| Bringing new tools, techniques, and emerging technology into the team's day-to-day practice, and helping teammates adopt what proves out. | Parity checker `scripts/databricks_parity.py` + `results/databricks_parity.json`; team adoption not shown. | **U** |

## What you bring

| JD line (verbatim) | Evidence | Label |
|---|---|---|
| 3+ years of hands-on experience with ETL pipelines, Python, and SQL. | Repo shows ETL + Python + SQL (dbt models `dbt/models/**`, event-log SQL in `databricks/fingerprint_task.py`). Years are not shown by a repo. | skills **C**; years **U** |
| 5+ years of proven expertise creating end-to-end data engineering pipelines handling large data volumes to support data science and business analytics. | One end-to-end pipeline, ~1.18M unpivoted rows (`docs/databricks-notes.md` "Observed gaps"). | pipeline **C**; years/volume **U** |
| Proficiency in PySpark, Scala-Spark, and scripting languages, with Databricks and AWS Glue expertise. | PySpark: `jobs/transform.py`. Databricks: `databricks/`, `databricks.yml`. Scripting: `Makefile`, `scripts/`. Scala and Glue: not in repo. | PySpark/Databricks **C**; Scala **U**; Glue **U** |
| Proficient understanding and practical experience with cloud-based Big Data technologies — AWS EMR, Apache Spark, Databricks, AWS Redshift, S3 — and orchestration tools such as Apache Airflow / AWS Step Functions. | Spark, Databricks, Airflow: above. S3: config paths and `s3a://` support in `jobs/io_utils.py`, but the DAG download is a TODO stub and README claims were narrowed (7e2785e), so S3 was not exercised end to end. EMR, Redshift, Step Functions: not shown. DuckDB as warehouse is the nearest analogue to Redshift (**I**, not equivalence). | Spark/Databricks/Airflow **C**; S3 partial **C**; EMR/Redshift/Step Functions **U** |
| Hands-on experience with technical design, development, conducting code reviews, and driving code quality. | ADR 0001, ruff in CI (`.github/workflows/ci.yml:21-24`), pytest in CI (`:57-60`), PR history (#5-#7). Reviewing others' code is not shown; 0330ffb is review of own code. | design/quality **C**; reviewing others **U** |
| Experience with log monitoring and error-notification tools such as Datadog / CloudWatch and Grafana. | Only a logging `_on_failure_callback` (`dags/rental_pipeline_dag.py:37`) and the Databricks event-log query. No Datadog, CloudWatch, Grafana. | **U** (not shown); event log **C** |
| Proven experience in cloud engineering, including provisioning AWS services with Terraform and CloudFormation. | None in repo. `databricks.yml` (bundles) declares Databricks resources as code (**I**, not equivalence). | **U** (not shown) |
| Strong understanding of DevOps practices, CI/CD pipelines, agile methodologies, and version control systems (Git). | GitHub Actions lint + test jobs (`ci.yml`), lockfile drift check (44300cb), feature-branch + PR flow. Agile: not shown. | CI/Git **C**; agile **U** |
| Proven ability to work through technical challenges and complex problem-solving ... digging to root cause rather than treating symptoms ... | Failure modes 1-4; 3149225 (null-containing keys miscounted by `countDistinct`) with `tests/test_parity.py`; a73e2eb row checksum added after the parity gap. | **C** |
| Experience with both functional and non-functional requirements, including security, performance, and scalability. | Functional: tests + DQ. Non-functional: see above. | functional **C**; NFR **U** |
| Demonstrated use of AI tools as an assistant in your engineering workflow, with the judgment to know where they help ... | As the AI row above. | files **C**; judgment **U** pending diff |
| Strong technical expertise in data architecture and data integrations ... / Excellent communication skills ... / Comfort working as part of a cross-functional fusion team ... | Architecture: README diagram, ADR. Communication/teamwork: not a repo artifact. | architecture **C**; rest **U** |

## Added bonus

| JD line (verbatim) | Evidence | Label |
|---|---|---|
| Java and Java-streaming development for real-time / near-real-time processing and event-driven architectures. / Experience building AI/ML data pipelines ... / Experience building with LLMs or AI agents in production ... | CI sets up Java 11 only for Spark. No streaming, ML, feature engineering, or LLM code. | **U** (not shown) |
| Experience using GitHub Copilot within secure development workflows ... | Not shown (Claude Code / Codex, not Copilot). | **U** |
| AWS Certified Developer (Associate) or equivalent ... / Supply chain experience. / Demonstrated experience transforming large-scale applications. | Not a repo artifact. | **U** |

## Gaps to say plainly (one line each)

- **Terraform / CloudFormation:** not shown in this repo. Closest: Databricks Asset Bundles (`databricks.yml`) declare the job and pipeline as code. **I**, one platform, not equivalence.
- **Datadog / CloudWatch / Grafana:** not shown. Closest: the pipeline event log queried via SQL (`databricks/fingerprint_task.py`). **I**.
- **Scala-Spark:** not shown. PySpark only.
- **AWS Glue / EMR / Redshift / Step Functions:** not shown. Closest: Databricks serverless (compute), DuckDB (warehouse), Airflow (orchestration). **I**.
- **S3 end to end:** code paths exist, never exercised here; the DAG download is a stub.
- **Scale and enterprise data:** public CSV, ~1.18M unpivoted rows, one pipeline.

## Note on the posting

The posting states Toyota will not sponsor work authorization now or in the future. Not a repo matter; flagged so you have checked it against your own situation before the interview.
