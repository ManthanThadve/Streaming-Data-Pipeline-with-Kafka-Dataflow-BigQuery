# sky-data-platform-data-formatter-tibco

The Data Formatter TIBCO Dataflow is designed for reading tibco data from Bigquery Raw Layer in batch, apply formatting rules then write into the Formatted Layer.

# Conversion
The aim of this step is to map, for each row read from the raw table, the column name from capital case to snake case, which is the naming convention used in the Formatted table.


# Build FlexTemplate

## Create jar file
Launch the following command from the project root to compile the code and package it as a jar.
```shell
mvn clean package
```

## Create the FlexTemplate
Launch the following commands to set the environment variables:
```shell
export TEMPLATE_PATH=gs://de-nc-st-bu-data-platform-sandbox-datateam/dataflow-templates/data-formatter-tibco-template.json
```
```shell
export TEMPLATE_IMAGE=gcr.io/de-nc-gr-pr-datateam/data-formatter-tibco:latest
```
Run the following command from the project root to build the flex template
```shell
	gcloud dataflow flex-template build $TEMPLATE_PATH \
	--image-gcr-path "$TEMPLATE_IMAGE" \
	--sdk-language "JAVA" \
	--flex-template-base-image JAVA11 \
	--metadata-file "metadata.json" \
	--jar "target/sky-dp-data-platform-data-formatter-tibco-x.x.x-SNAPSHOT.jar" \
	--env FLEX_TEMPLATE_JAVA_MAIN_CLASS="de.sky.dataplatform.FormatterPipeline"
```
# Run the dataflow

## Arguments
The required arguments are:
- **`bucket`**: the GCS bucket that contains the entity configuration files.
- **`entityDataConf`**: the GCS path that contains information about input topic and output table for a given entity.
- **`queryStartDate`**: start of the time range used for the query.
- **`queryEndDate`**: end of the time range used for the query.
- **`inputTable`**: the raw table where the data are read .
- **`outputTable`**: the formatted table where the data are written .
- **`timeDelay`**: number of days to be subtracted from the start date to obtain the range to check on the formatted.
- **`entityName`**: Name of entity

Before running, one should export the `GOOGLE_APPLICATION_CREDENTIALS` env variable containing the path to the service account credentials

## Running with DataflowRunner
```shell
mvn compile exec:java -Dexec.mainClass=mt.netflix.dataplatform.FormatterPipeline \
-Dexec.args=" \
--runner=DirectRunner \
--inputFolderPath=M:\\Training\\Beam\\Datasets\\Netflix \
--inputFileName=europe-netflix_shows.csv \
--outputFolderPath=M:\\Training\\Beam\\Datasets\\Netflix\\output \
--outputFileName=netflix-output \

```

# Run on Airflow
To run the dataflow job trigger the Airflow dag data-formatter-tibco on Composer that runs the flexTemplate built previously.