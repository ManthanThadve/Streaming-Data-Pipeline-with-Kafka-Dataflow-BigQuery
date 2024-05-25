package mt.netflix.dataplatform;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import mt.netflix.dataplatform.options.FormatterPipelineOptions;
import mt.netflix.dataplatform.transforms.KafkaRecordsEnrich;
import mt.netflix.dataplatform.utils.GenericRecordSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class KafkaFormatterPipeline {
    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaFormatterPipeline.class);

    public static void main(String[] args) throws IOException {

        System.out.println("IN main Function");

        FormatterPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as
                (FormatterPipelineOptions.class);

        System.out.println("Pipeline Options are set");
        System.out.printf("'%s', '%s', '%s', '%s'",options.getBootstrapServer(), options.getSchemaRegistry(), options.getInputTopic(), options.getInputTopic());

        String startMessage = String.format("Started Reading data from '%s' and write to '%s' at time %s", options.getInputTopic(), options.getOutputTopic(), new Timestamp(System.currentTimeMillis()));

        LOGGER.info(startMessage);
        run(options);

    }
    public static void run(FormatterPipelineOptions options) throws IOException {

        LOGGER.info("In Run function");

        Pipeline p = Pipeline.create(options);

        String enrichedSchemaPath = options.getEnrichSchemaPath().toString();

        LOGGER.info("Pipeline created");

        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
//        kafkaConsumerConfig.put("bootstrap.servers", "broker:29092");
        kafkaConsumerConfig.put("schema.registry.url", "http://localhost:8081");
        kafkaConsumerConfig.put("specific.avro.reader", "true");
        kafkaConsumerConfig.put("auto.offset.reset", "earliest");
        kafkaConsumerConfig.put("group.id", "beam-avro-consumer-group");

        Map<String, Object> kafkaProducerConfig = new HashMap<>();
        kafkaProducerConfig.put("bootstrap.servers", "localhost:9092");
        kafkaProducerConfig.put("key.serializer", StringSerializer.class.getName());
        kafkaProducerConfig.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProducerConfig.put("schema.registry.url", "http://localhost:8081");

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

        PCollection<KV<String, GenericRecord>> enriched_records = kafka_events.apply(new KafkaRecordsEnrich(enrichedSchemaPath));

        LOGGER.info("kafka write");

        enriched_records.apply("Write to Kafka",
                KafkaIO.<String, GenericRecord>write()
                        .withBootstrapServers("localhost:9092")
                        .withTopic(options.getOutputTopic().toString())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(GenericRecordSerializer.class)
                        .withProducerConfigUpdates(kafkaProducerConfig)
        );

        PipelineResult result = p.run(options);

        LOGGER.info("Pipeline RUN");

    }
}
