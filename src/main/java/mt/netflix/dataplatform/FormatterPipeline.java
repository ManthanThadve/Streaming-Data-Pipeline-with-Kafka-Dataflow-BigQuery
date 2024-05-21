package mt.netflix.dataplatform;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import mt.netflix.dataplatform.options.FormatterPipelineOptions;
import mt.netflix.dataplatform.transforms.EnrichGenericRecordsFn;
import mt.netflix.dataplatform.utils.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class FormatterPipeline {

    public static final Logger LOGGER = LoggerFactory.getLogger(FormatterPipeline.class);

    public static void main(String[] args) throws IOException {

        System.out.println("IN main Fucntion");

        FormatterPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as
                (FormatterPipelineOptions.class);

        System.out.println("Pipeline Options are set");

        String startMessage = String.format("Started Reading data from '%s' and write to '%s' at time %s", options.getInputTopic(), options.getOutputTopic(), new Timestamp(System.currentTimeMillis()));

        LOGGER.info(startMessage);
        run(options);

    }

    public static void run(FormatterPipelineOptions options) throws IOException {

        System.out.println("In Run function");

        Pipeline p = Pipeline.create(options);

        System.out.println("Pipeline created");

        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
        kafkaConsumerConfig.put("bootstrap.servers", "localhost:9092");
//        kafkaConsumerConfig.put("key.deserializer", StringDeserializer.class.getName());
//        kafkaConsumerConfig.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        kafkaConsumerConfig.put("schema.registry.url", "http://localhost:8081");
        kafkaConsumerConfig.put("specific.avro.reader", "true");
        kafkaConsumerConfig.put("group.id", "beam-avro-consumer-group");
        System.out.println("consumerConfig created");
        // Define Kafka producer configuration
        Map<String, Object> kafkaProducerConfig = new HashMap<>();
        kafkaProducerConfig.put("bootstrap.servers", "localhost:9092");
        kafkaProducerConfig.put("key.serializer", StringSerializer.class.getName());
        kafkaProducerConfig.put("value.serializer", KafkaAvroSerializer.class.getName());
        kafkaProducerConfig.put("schema.registry.url", "http://localhost:8081");

        System.out.println("ProducerConfig created");

        Schema netflix_schema = AvroSchemaGenerator
                .generateAvroSchema(options.getRawSchemaPath().toString());

        String enrichedSchemaPath = options.getEnrichSchemaPath().toString();


        LOGGER.info("******************** Reading of CSV file started *********************");
        LOGGER.info("******************** Parsing of CSV data started *********************");

        System.out.println("logger");

        PCollection<KafkaRecord<String, GenericRecord>> kafka_events;
        kafka_events = p
                .apply("KafkaConsumer",
                        KafkaIO.<String, GenericRecord>read()
                                .withBootstrapServers("localhost:9092")
                                .withTopic(options.getInputTopic().toString())
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(
                                        ConfluentSchemaRegistryDeserializerProvider
                                                .of("http://localhost:8081", options.getInputTopic().toString()+"-value")
                                )
                                .withConsumerConfigUpdates(kafkaConsumerConfig)
                );

        System.out.println("kafka read");

        PCollection<KV<String, GenericRecord>> avro_records = kafka_events
                .apply("Add extra fields",
                        ParDo.of(new EnrichGenericRecordsFn(enrichedSchemaPath)));

        System.out.println("tranformation");

        avro_records.apply("Write to Kafka",
                KafkaIO.<String, GenericRecord>write()
                        .withBootstrapServers("localhost:9092")
                        .withTopic(options.getOutputTopic().toString())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer((Class) KafkaAvroSerializer.class)
                        .withProducerConfigUpdates(kafkaProducerConfig)
        );

        System.out.println("kafka write");

        p.run(options);

        System.out.println("Pipeline RUN");
//        System.exit(0);

    }
}
