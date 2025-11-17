## Local Engine Setup

### DuckDB

- Copy `.env.dev_duckdb` and adjust `FF_DUCKDB_PATH` if you want a different location (default: `.local/api_demo.duckdb`).  
  Optionally set `FF_DUCKDB_SCHEMA` (default schema for models/seeds) and `FF_DUCKDB_CATALOG` (catalog alias) if you need to isolate namespaces.
- Create the target directory once: `mkdir -p examples/api_demo/.local`.
- Run `make ENGINE=duckdb seed run` to build the seeds and models inside the DuckDB file.

### Postgres

- Start a local database, e.g. via Docker:  
  `docker run --name fft-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15`.
- Set `FF_PG_DSN` in `.env.dev_postgres` (for example `postgresql+psycopg://postgres:postgres@localhost:5432/fft`) and optionally override `FF_PG_SCHEMA` (defaults to `api_demo`).  
  The executor ensures the schema exists via `CREATE SCHEMA IF NOT EXISTS` on first connect.
- Execute `make ENGINE=postgres seed run` to materialize seeds and models in Postgres.

### Databricks Spark (local)

- Install Java (JDK ≥ 17) and declare `JAVA_HOME`, for example:  
  `brew install openjdk@17`  
  `echo 'JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> examples/api_demo/.env.dev_databricks`.
- Optionally tweak `FF_SPARK_MASTER` / `FF_SPARK_APP_NAME` in `.env.dev_databricks` (default: `local[*]`).
- To persist tables across separate `seed`/`run` sessions, enable the bundled Hive metastore defaults:  
  `FF_DBR_ENABLE_HIVE=1`, `FF_DBR_WAREHOUSE_DIR=examples/api_demo/spark-warehouse`, `FF_DBR_DATABASE=api_demo`.
- Switch the physical format by setting `FF_DBR_TABLE_FORMAT` (e.g. `delta`, requires the Delta Lake runtime); extra writer options can be supplied via `profiles.yml → databricks_spark.table_options`.
- Ensure your shell loads `.env.dev_databricks` (via `make`, `direnv`, or manual export) and run `make ENGINE=databricks_spark seed run`.


Yep, let’s bolt on a “how to set it up in GCP” section that fits with what you already have.

Here’s an extended BigQuery section you can drop into your docs (you can keep or trim the parts you already added):

### BigQuery

#### 1. One-time setup in Google Cloud

You only need to do this once per project / environment.

1. **Create (or pick) a GCP project**

   - Go to the *Google Cloud Console* → **IAM & Admin → Create project**.
   - Give it a name, e.g. `FFT Basic Demo`, and note the **Project ID**, e.g. `fft-basic-demo`.
   - All further steps refer to this project id.

2. **Enable the BigQuery API**

   - In the console, go to **APIs & Services → Library**.
   - Search for **“BigQuery API”** and click **Enable**.
   - (Optional but recommended) Also enable **BigQuery Storage API** for faster reads.

3. **Create a BigQuery dataset**

   - Go to **BigQuery** in the console (left sidebar).
   - Make sure your project `fft-basic-demo` is selected.
   - Click **“+ Create dataset”**:
     - **Dataset ID**: e.g. `basic_demo`
     - **Location type**: choose a **multi-region**, e.g.:
       - `EU` or `US`
     - Click **Create dataset**.

   ⚠️ **Important:** The dataset **location must match** the location you use in your env (`FF_BQ_LOCATION`).
   - If your dataset is in `EU` (multi-region), then `FF_BQ_LOCATION=EU`.
   - If the dataset is in a single region like `europe-west3`, use that exact region name.

4. **Create a service account (for CI / non-interactive use)**

   For local dev you can use your own user credentials (see below), but for CI/CD or shared environments
   a service account is better.

   - Go to **IAM & Admin → Service Accounts → Create service account**.
   - Name it e.g. `fft-runner`.
   - On the **Roles** step, add roles with BigQuery write access, for example:
     - `BigQuery Job User`
     - `BigQuery Data Editor`
   - (Optionally) Restrict to dataset level later if you want stricter permissions.

   Then create a key:

   - Click your service account → **Keys → Add key → Create new key**.
   - Select **JSON**, download the file, and store it somewhere safe (e.g. `~/.config/gcloud/fft-sa.json`).

5. **Authentication options**

   You have two ways to authenticate locally:

   **A) Application Default Credentials via gcloud (easy for dev)**

  ```bash
  gcloud auth application-default login
  ```

This opens a browser, you log in, and Google stores your ADC in
`~/.config/gcloud/application_default_credentials.json`.

The BigQuery client in `fastflowtransform` will pick this up automatically **as long as**
`FF_BQ_PROJECT` points to a project you have access to.

**B) Service account key (good for CI)**

* Put the downloaded JSON key (from step 4) somewhere on disk.

* Set the environment variable before running `fft`:

  ```bash
  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/fft-sa.json
  ```

* Make sure the service account has at least:

  * `BigQuery Job User`
  * `BigQuery Data Editor`

* Optionally grant `BigQuery Data Viewer` if you’re only reading some tables.

---

#### 2. Local configuration (env + profiles)

1. **Environment file (`.env.dev_bigquery`)**

   ```env
   # BigQuery connection
   FF_BQ_PROJECT=fft-basic-demo         # your GCP project id
   FF_BQ_DATASET=basic_demo             # dataset from step 3
   FF_BQ_LOCATION=EU                    # or europe-west3, US, etc. MUST match dataset location

   # Active fft environment name (must match profiles.yml)
   FFT_ACTIVE_ENV=dev_bigquery
   ```

   Load this via `direnv`, `make`, or manual `export`.

2. **profiles.yml**

   ```yaml
   dev_bigquery:
     engine: bigquery
     bigquery:
       project: ${FF_BQ_PROJECT}
       dataset: ${FF_BQ_DATASET}
       location: ${FF_BQ_LOCATION}
       use_bigframes: true  # Python models use BigQuery DataFrames (BigFrames)
   ```

---

#### 3. Running seeds, models, and tests

* **Seed BigQuery from `seeds/`:**

  ```bash
  make ENGINE=bigquery seed
  ```

  This writes all `seeds/*.csv|parquet` to tables under
  `${FF_BQ_PROJECT}.${FF_BQ_DATASET}.*`.

* **Build models:**

  ```bash
  make ENGINE=bigquery run
  ```

  * SQL models are executed as BigQuery queries.
  * Python models with `only="bigquery"` run via `BigQueryBFExecutor` (BigQuery DataFrames)
    and are written back into the same dataset.

* **Run data-quality tests:**

  ```bash
  make ENGINE=bigquery test
  ```

  `fft test` uses the BigQuery shim to run checks like `not_null`, `unique`,
  `row_count_between`, `greater_equal`, etc. against
  `${FF_BQ_PROJECT}.${FF_BQ_DATASET}.<table>`.

---

#### 4. Common BigQuery gotchas

* **Location mismatch**

  * Error like `Location basic_demo does not support this operation` or `Not found: Dataset ...`:

    * Check the **dataset location** in the BigQuery UI.
    * Make sure `FF_BQ_LOCATION` is exactly that value (`EU`, `US`, `europe-west3`, …).
    * Ensure the executor is initialized with the same location (via `profiles.yml` → `location`).

* **Permission issues**

  * If you see `accessDenied` or `Permission denied`:

    * Confirm you authenticated (ADC or service account).
    * Ensure your user / service account has at least:

      * `BigQuery Job User`
      * `BigQuery Data Editor` on the project or dataset.

* **Dataset not found**

  * Error `Not found: Dataset fft-basic-demo:basic_demo`:

    * Check that the dataset id matches exactly:

      * Project: `fft-basic-demo`
      * Dataset: `basic_demo`
    * Verify it exists and is in the same project you set in `FF_BQ_PROJECT`.
