# Dog Aging Ingest
Batch ETL pipeline to mirror Dog Aging Project (DAP) data into
the Terra Data Repository (TDR).

DAP is a 10-year longitudinal study, eventually integrating multiple data streams.
For now, we're focused on a single survey from their RedCap data stream. The extraction
and transformation pipelines for HLES Baseline are implemented. We still need to
define & deploy the automated orchestration component for that data stream.

### Handling New Data
DAP's various data streams will spin up, refresh, and turn down at semi-independent
cadences. They will all need to flow into the same dataset (for ease of access control),
but that doesn't mean they need to be processed as a single workflow. A more scalable
pattern would likely be to:
1. Use TDR schema migration capabilities to add new tables to the long-lived dataset
   when needed
2. Set up distinct ETL workflows for each independent data stream

As an initial step towards this pattern, the extraction and transformation programs
for DAP's data stream (the HLES baseline) are nested under their own [`hles/`](dap-etl)
subdirectory.

## HLES Baseline
The HLES "baseline" survey is the entry-point for all DAP participants. It is a relatively
sparse dataset, with over 2000 possible (and usually mostly-null) variables. Dog owners
complete this survey once as they enter the project.

### Schema Design
We designed our version of the HLES schema with the following goals in mind:
* "Clean up" inconsistencies in column names where possible
* Reduce the magnitude of columns captured in the dataset
* Eliminate array-valued columns, for compatibility with common statistical applications
* Keep the total count of tables low, to keep the barrier to entry low for users new to
  cloud processing / Terra

Together, these principles led us to decide:
* Field names from the source data are renamed for consistency / accuracy, but retain
  a prefix indicating their RedCap instrument
* The single HLES record-per-dog pulled from RedCap are split into rows in 2-4 tables:
  1. An `hles_owner` table containing all owner-specific information (i.e. education, ethnicity).
     Splitting out owner info prepares us for a future where dogs transfer ownership.
  2. An `hles_dog` table containing _most_ dog-specific information, along with a foreign key
     to the owner table. The only data not included in this table is the "long tail" of health
     condition details.
  3. An `hles_cancer_condition` table containing all the details around cancer diagnoses, with
     a foreign key to the dog table. The HLES questions around cancer use an entirely different
     pattern from other conditions, and splitting the data into a separate table was the simplest
     way for us to deal with it.
  4. An `hles_health_condition` table containing the details of all non-cancer conditions, with
     a foreign key to the dog table. We reduce the cardinality of the entire dataset in this table
     by transposing the survey's 2K+ distinct questions about different conditions into a common
     set of columns, with a row in the table per dog/condition pair.
* Array-valued columns from the source data (multi-selects) are "unrolled" into multiple
  top-level BOOLEAN columns

### Data Extraction
We use a Dataflow pipeline to extract HLES questions from RedCap. The pipeline filters records to
ensure that only complete, consented data is downloaded. The resulting data is written out in "EAV"
form, with fields:
* "record": The participant's ID across the entire study
* "redcap_event_name": An marker for the wave / round of surveys the data point was pulled from
* "field_name": The raw key of the question
* "value": The user-provided answer to the question

Note that:
1. Multi-select columns can have multiple data points for the same record/event/field trio, appearing
   as separate rows in the output
2. Answers to categorical questions will be exported in "raw" numeric form

The extraction pipeline can also download the JSON of each instrument's data dictionary.

#### Authenticating with RedCap
The extraction pipeline requires a RedCap auth token as input. We've stashed our tokens in Vault.
To get the token, run this shell command:
```bash
ENV=dev # ENV=prod also works
vault read -field=token secret/dsde/monster/${ENV}/dog-aging/redcap-tokens/automation
```
NEVER check the token into source control.

### Data Transformation
Extracted HLES data is transformed using another Dataflow pipeline. The pipeline includes _a lot_ of
code because of all the basic column-renamings and the full listing of health conditions. On the plus
side, we've been told by DAP leadership that the HLES structure is effectively immutable on their end,
so it's unlikely we'll ever need to rewrite large chunks of this processing logic.

## Additional Datasets

### CSLB Data
The Canine Social and Learned Behavior is a followup survey to HLES that is administered annually. 
The goal of this survey is to assess age-related cognitive and behavioral changes in dogs. The first 
time a participant fills out the CSLB survey, a baseline score is established. Participants will 
have the opportunity to complete the survey again every year. This repeated design allows us to learn 
how the dogs in the study change over time. Answers to all questions are required.

#### CSLB Extraction Criteria
* Pull all records from each yearly arm *"annual_{yyyy}_arm_1"*
where _canine_social_and_learned_behavior_complete_ is marked as complete

#### CSLB Schema Design
* A single `cslb` table which contains a single CSLB record per dog per year. 
* The table includes the 16 CSLB question responses and a foreign key to the dog table. 


## Environmental Data
Environmental Data in the Dog Aging Project is published monthly to RedCap for each dog using the addresses
collected in the HLES survey. It seeks to capture a snapshot of key environmental factors that may affect a 
dog's health over time so it can be used in conjunction with the other datasets. All fields from environmental 
data are calculated using the HLES primary and secondary addresses.

#### Environmental Extraction Criteria
* Pull all records from each monthly arm for each address: *"annual_{MMMyyyy}_arm_1"*, *"annual_{MMMyyyy}_secondary_arm_1* 
* where _baseline_complete_ is marked as complete

#### Environment Schema Design
Environmental data is modeled closely on what the DAP provided us, with a proto-schema which we
converted to a repo schema. We decided to break the larger table up into smaller fragments: 
_geocoding, census, pollutants, temperature_precipitation, and walkability_ variables.

* A single `environment` table which contains a single Environment record per dog per month per address. 
* The table includes all environmental variables and a foreign key to the dog table. 
* Rows in the final Environment table will be unique on _dog_id_ and _address_month_year_.


## Sample Data
Sample Data in the Dog Aging Project is populated as different cohorts of participants are sent different 
sample kits. Our extraction of this data seeks to capture the linkage between study_id (_dog_id_) and 
sample_id as well as some other metadata about the sample.

#### Sample Extraction Criteria
* Pull all records from the baseline arm (*baseline_arm_1*) 
* where _k1_tube_serial_ and _k1_rtn_tracking_date_ must be populated

#### Sample Schema Design
* A single `sample` table which contains multiple samples per dog.
* Simple lookup table with 6 fields and a foreign key to the dog table.
* Rows in the final Environment table will be unique on _dog_id_ and _sample_id_.


## Label Mapping Table
We have a manually maintained lookup table that we are constructing using the Terra JSON schema for the DAP tables and 
the DAP data dictionaries provided from RedCap. The table contains a row for each field in the the final dog aging tables.
The mapping table includes fields to capture information about the upstream source of data, datatypes, and most importantly,
the mapping of raw values ot value labels for RedCap survey questions with *radio* responses.

We are looking forward to building some tooling that will be able to
parse a proto-schema provided by DAP into usable code for ingest in the form of a Terra JSON Schema output.


#### Creating Outfiles 
DAP's long term future would include an official TDR<->Workspace integration but we are currently still building that out.
Until that integration is live, we've written a [script](./hack/convert-output-to-tsv.py) to convert
the outputs of our transformation pipeline into workspace-compatible TSVs. 
The script takes two positional arguments:
1. Path to the top-level directory containing the outputs of the transformation pipeline
2. Path to a directory where converted TSVs should be written
There are additional arguments to that script to add some flexibility to the tool:
* List of tables to process (currently looks for all tables)
* Whether to output primary keys with a modified column name for Firecloud compatibility: *entity:{table_name}_id*<br class="Apple-interchange-newline">
* Can output primary keys with a modified column name for Firecloud compatibility: *entity:{table_name}_id*
* Call Format:
    * `python convert-output-to-tsv.py {transform GS path} {tsv outfile local path} -d`
    * `python convert-output-to-tsv.py gs://example-bucket/weekly_refresh/20210525/transform /Users/DATA/DAP/tsv -d`

#### Validation
We have a set of validation tests that we run to QA new TSVs. These validation tests are used to look for specific data
issues that were discovered in creating the curated 2020 refresh in coordination with the DAP team.

## Refreshing Data
### Running the pipelines on GCS
While it is possible to manually process the DAP data on our local machines, it had become too cumbersome to do so once we 
crossed the threshold of around 10k+ participants. We pushed our extraction and transformation logic out to the cloud and 
are now running our refreshes in *dev_mode* using the dataflow_beam runner.
The Dagster project we added for DAP includes 3 different modes of operation:
1. `local_mode` uses local_beam_runner
1. `dev_mode` uses dataflow_beam_runner
1. `prod_mode` uses dataflow_beam_runner

For now, we can call individual pipelines manually with any runner and then call the tsv script to write files locally.
The general structure for the SBT call to kick off one of our pipelines:
* `sbt "{scala_project} {target_class} {sbt parameters} {dataflow parameters}"`
* Here is sample an sbt for the Environment extraction pipeline:
    >sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.environment.EnvironmentExtractionPipeline 
    --apiToken=foo --pullDataDictionaries=false --outputPrefix=bar --runner=dataflow --project=baz --region=us-central1
    --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 
    --experiments=shuffle_mode=service --endTime=qux"

Alternatively we can also execute a Dagster pipeline which will run all the pipelines and write local TSVs and will 
use a runner based on the mode.

### Manual Refresh (Dagster)
1. Navigate to the *orchestration* subdirectory
2. Setup python virtual environment
    * `poetry install` to install your project’s package
    * `poetry update` to get the latest versions of the dependencies and to update the poetry.lock file
2. Update config entries in orchestration/dev_refresh.yaml:
    * Update the `refresh_directory` resource config entry (_this should be a GCS path_)
    * Update the `working_dir` config entry for the *write_outfiles* solid
    * Update the `end_time`, `pull_data_dictionaries`, and `api_token` config entries for each extract solid
3. Execute the pipeline:
    * `poetry shell` spawns a shell within the project's virtual environment
    * `dagster pipeline execute -f repositories.py -a repositories -p refresh_data_all --mode dev -c dev_refresh.yaml`
4. Files should appear within a "tsv_output" relative to the `working_dir` specified in the config.

### Data Delivery to DAP
When the hles_data volume was much smaller, we were able to load the final TSVs directly to Terra using the UI prompts.
Once the data grew too big to load via browser UI, we started utilizing the field engineering script to upload large entities. 
While still building out the TDR <-> workspace integration for DAP, we are using a Google bucket to deliver refreshed TSVs.
