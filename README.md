# Run the dataflow

## Arguments
The required arguments are:
- **`runner`**: Runner on which pipeline should execute.
- **`rawSchemaPath`**: Path to schema file of raw schema .
- **`bootstrapServer`= Bootstrap Server of the kafka broker.
- **`schemaRegistry`= URL of the confluent kafka Schema Registry.
- **`inputTopic`**: start of the time range used for the query.
- **`enrichSchemaPath`**: Path to schema file of enriched schema .
- **`outputTopic`**: the enrich topic where the data is being write .

Before running, one should export the `GOOGLE_APPLICATION_CREDENTIALS` env variable containing the path to the service account credentials

## Running with DirectRunner
```shell
mvn compile exec:java -Dexec.mainClass=mt.netflix.dataplatform.KafkaFormatterPipeline \
-Dexec.args=" \
--runner=DirectRunner \
--bootstrapServer=localhost:9092 \
--schemaRegistry=http://localhost:8081 \
--rawSchemaPath=M:\\Training\\Beam\netflix-kafka-etl-streaming\\configurations\\schema\\raw_netflix.avsc \
--inputTopic=netflix_events \
--enrichSchemaPath=M:\\Training\\Beam\netflix-kafka-etl-streaming\\configurations\\schema\\enriched_netflix.avsc \
--outputTopic=netflix_enriched \



```

## Running with DirectRunner to write into snowflake
```shell
mvn compile exec:java -Dexec.mainClass=mt.netflix.dataplatform.KafkaFormatterPipeline \
-Dexec.args=" \
--runner=DirectRunner \
--bootstrapServer=localhost:9092 \
--schemaRegistry=http://localhost:8081 \
--enrichSchemaPath=M:\\Training\\Beam\\Streaming-Data-Pipeline-with-Kafka-Dataflow-BigQuery\\configurations\\schema\\enriched_netflix.avsc \
--inputTopic=raw_event_store \
--username=ManthanThadve \
--password=Manthan@123 \
--serverName=xp60388.central-india.azure.snowflakecomputing.com \
--database=DATA_PLATFORM \
--schema=NETFLIX \
--warehouse=COMPUTE_WH \
--role=ACCOUNTADMIN \



```