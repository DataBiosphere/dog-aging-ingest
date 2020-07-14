# Dog Aging Ingest
Batch ETL pipeline to mirror Dog Aging Project (DAP) data into
the Terra Data Repository (TDR).

## Status: WIP
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
for DAP's data stream (the HLES baseline) are nested under their own [`hles/`](./hles)
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

The extraction pipeline will also download the JSON of each instrument's data dictionary. We intend
to use this to reconstruct a lookup table in the TDR dataset.

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

### Hack: Uploading Results to Terra
The TDR will eventually be fully integrated with Terra Workspaces. In the immediate future, however,
there's no "golden path" for bridging data from the repository into the workspace model. DAP's near-term
users aren't very technical, so it's unlikely they'll be comfortable figuring things out on their own.

To give ourselves some breathing room, we've written a [script](./hack/convert-output-to-tsv.py) to convert
the outputs of our transformation pipeline into workspace-compatible TSVs. We can manually run the script
after the two Dataflow pipelines as a final transformation step, then upload the outputs to a workspace.

The script takes two positional arguments:
1. Path to the top-level directory containing the outputs of the transformation pipeline
2. Path to a directory where converted TSVs should be written

Manually processing data on our local machines obviously isn't ideal, but it should scale well enough
as long as HLES stays on the order of a few thousand participants. Once we cross into the area of 10K+
participants, we'll need to either push the transformation logic into the cloud, or (ideally) stop doing
things manually altogether in favor of the official TDR<->Workspace integration.
