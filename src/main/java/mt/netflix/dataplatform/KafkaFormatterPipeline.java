package mt.netflix.dataplatform;

import mt.netflix.dataplatform.options.FormatterPipelineOptions;
import mt.netflix.dataplatform.transforms.KafkaRecordsEnrich;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestamp;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumber;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeText;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO.DataSourceConfiguration;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static org.apache.beam.sdk.io.snowflake.enums.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.snowflake.enums.WriteDisposition.APPEND;

public class KafkaFormatterPipeline {
    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaFormatterPipeline.class);

    public static void main(String[] args) throws IOException {

        System.out.println("IN main Function");

        FormatterPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as
                (FormatterPipelineOptions.class);

        SnowflakeIO.DataSourceConfiguration datasource = SnowflakeIO.DataSourceConfiguration.create()
                .withUsernamePasswordAuth(
                        options.getUsername(),
                        options.getPassword())
                .withServerName(options.getServerName())
                .withDatabase(options.getDatabase())
                .withRole(options.getRole())
                .withWarehouse(options.getWarehouse())
                .withSchema(options.getSchema());

        LOGGER.info("Pipeline Options are set");
        System.out.printf("'%s', '%s', '%s'\n",options.getBootstrapServer(), options.getSchemaRegistry(), options.getInputTopic());

        String startMessage = String.format("Started Reading data from '%s' and write to Snowflake database %s at time %s", options.getInputTopic(), options.getDatabase() ,new Timestamp(System.currentTimeMillis()));

        LOGGER.info(startMessage);
        run(options, datasource);

    }
    public static void run(FormatterPipelineOptions options, DataSourceConfiguration dataSourceConf) throws IOException {

        LOGGER.info("In Run function");

        Pipeline p = Pipeline.create(options);

        String enrichedSchemaPath = options.getEnrichSchemaPath().toString();

        SnowflakeIO.UserDataMapper<GenericRecord> userDataMapper =
                (GenericRecord row) -> new Object[] {};


        SnowflakeTableSchema tableSchema =
                new SnowflakeTableSchema(
                        SnowflakeColumn.of("title", new SnowflakeText(), true),
                        SnowflakeColumn.of("main_cast", new SnowflakeText(),true),
                        SnowflakeColumn.of("country", new SnowflakeText(),true),
                        SnowflakeColumn.of("date_added", new SnowflakeText(),true),
                        SnowflakeColumn.of("release_year", new SnowflakeNumber(),true),
                        SnowflakeColumn.of("rating", new SnowflakeText(),true),
                        SnowflakeColumn.of("duration", new SnowflakeText(),true),
                        SnowflakeColumn.of("listed_in", new SnowflakeText(),true),
                        SnowflakeColumn.of("description", new SnowflakeText(),true),
                        SnowflakeColumn.of("number_of_cast", new SnowflakeNumber(),true),
                        SnowflakeColumn.of("systemModStamp", new SnowflakeTimestamp(), true));


        LOGGER.info("Pipeline created");

        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
//        kafkaConsumerConfig.put("bootstrap.servers", "broker:29092");
        kafkaConsumerConfig.put("schema.registry.url", "http://localhost:8081");
        kafkaConsumerConfig.put("specific.avro.reader", "true");
        kafkaConsumerConfig.put("auto.offset.reset", "earliest");
        kafkaConsumerConfig.put("group.id", "beam-avro-consumer-group");

//        Map<String, Object> kafkaProducerConfig = new HashMap<>();
//        kafkaProducerConfig.put("bootstrap.servers", "localhost:9092");
//        kafkaProducerConfig.put("key.serializer", StringSerializer.class.getName());
//        kafkaProducerConfig.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
//        kafkaProducerConfig.put("schema.registry.url", "http://localhost:8081");

        LOGGER.info("******************** Reading of Kafka Records from Topic started *********************");

        LOGGER.info("logger");

        PCollection<KafkaRecord<String, GenericRecord>> kafka_events;
        kafka_events = p
                .apply("KafkaConsumer",
                        KafkaIO.<String, GenericRecord>read()
                                .withBootstrapServers(options.getBootstrapServer().toString())
                                .withTopic(options.getInputTopic().toString())
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(
                                        ConfluentSchemaRegistryDeserializerProvider
                                                .of(options.getSchemaRegistry().toString(), options.getInputTopic().toString()+"-value")
                                )
                                .withConsumerConfigUpdates(kafkaConsumerConfig)
                                .commitOffsetsInFinalize()
                );

        LOGGER.info("kafka read");

        PCollection<GenericRecord> enriched_records = kafka_events.apply(new KafkaRecordsEnrich(enrichedSchemaPath));

        LOGGER.info("kafka write");

//        enriched_records.apply("Write to Kafka",
//                KafkaIO.<String, GenericRecord>write()
//                        .withBootstrapServers("localhost:9092")
//                        .withTopic(options.getOutputTopic().toString())
//                        .withKeySerializer(StringSerializer.class)
//                        .withValueSerializer(GenericRecordSerializer.class)
//                        .withProducerConfigUpdates(kafkaProducerConfig)
//        );
        enriched_records.apply("Write to Snowflake",
                SnowflakeIO.<GenericRecord>write()
                        .withDataSourceConfiguration(dataSourceConf)
                        .to("RAW_NETFLIX")
                        .withWriteDisposition(APPEND)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withTableSchema(tableSchema)
                        .withStagingBucketName("s3://beam-snowflake/")
                        .withUserDataMapper(userDataMapper)
                        .withSnowPipe("snowPipeName")
    );

        PipelineResult result = p.run(options);

        LOGGER.info("Pipeline RUN");

    }
}
