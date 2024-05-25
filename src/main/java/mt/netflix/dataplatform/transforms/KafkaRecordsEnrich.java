package mt.netflix.dataplatform.transforms;

import mt.netflix.dataplatform.utils.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class KafkaRecordsEnrich extends PTransform<PCollection<KafkaRecord<String, GenericRecord>>, PCollection<KV<String, GenericRecord>>> {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaRecordsEnrich.class);
    String enrichSchemaPath;
    public KafkaRecordsEnrich(String enrichedSchemaPath) {
        this.enrichSchemaPath = enrichedSchemaPath;
    }


    @Override
    public PCollection<KV<String, GenericRecord>> expand(PCollection<KafkaRecord<String, GenericRecord>> input) {
        return input.apply("Print Kafka Record", ParDo.of(new DoFn<KafkaRecord<String, GenericRecord>, KV<String, GenericRecord>>() {

            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
                try {
                    KafkaRecord<String, GenericRecord> kafkaRecord = c.element();
                    if (kafkaRecord == null) {
                        LOG.error("KafkaRecord is null");
                        return;
                    }

                    long timestamp = kafkaRecord.getTimestamp();
                    int partition = kafkaRecord.getPartition();
                    long offset = kafkaRecord.getOffset();
                    String topic = kafkaRecord.getTopic();

                    KV<String, GenericRecord> kv = kafkaRecord.getKV();
                    String key = kv.getKey();
                    GenericRecord existingRecord = kv.getValue();
                    if (existingRecord == null) {
                        LOG.error("Existing record is null for key: " + key);
                        return;
                    }

                    Schema modifiedSchema = AvroSchemaGenerator.generateAvroSchema(enrichSchemaPath);

                    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(modifiedSchema);
                    recordBuilder.set("title", getSafeString(existingRecord, "title"));
                    recordBuilder.set("cast", getMainCast(getSafeString(existingRecord, "cast")));
                    recordBuilder.set("country", getSafeString(existingRecord, "country"));
                    recordBuilder.set("date_added", getSafeString(existingRecord, "date_added"));
                    recordBuilder.set("release_year", getSafeInt(existingRecord, "release_year"));
                    recordBuilder.set("rating", getSafeString(existingRecord, "rating"));
                    recordBuilder.set("duration", getSafeString(existingRecord, "duration"));
                    recordBuilder.set("listed_in", getSafeString(existingRecord, "listed_in"));
                    recordBuilder.set("description", getSafeString(existingRecord, "description"));
                    recordBuilder.set("source_timestamp", convertTimestamp(timestamp));
                    recordBuilder.set("source_topic", topic);
                    recordBuilder.set("source_partition", partition);
                    recordBuilder.set("source_offset", offset);

                    GenericRecord enrichedRecord = recordBuilder.build();
                    LOG.info("Processed record with offset [{}], partition [{}], enrichedRecord: {}", offset, partition, enrichedRecord);

                    c.output(KV.of(key, enrichedRecord));

                } catch (Exception e) {
                    LOG.error("Error Processing element", e);
                }

            }

            private String getSafeString(GenericRecord record, String fieldName) {
                Object value = record.get(fieldName);
                return value != null ? value.toString() : "";
            }

            private Integer getSafeInt(GenericRecord record, String fieldName) {
                Object value = record.get(fieldName);
                if (value == null) {
                    return 0; // default value for missing integers
                }
                if (value instanceof Integer) {
                    return (Integer) value;
                } else {
                    try {
                        return Integer.parseInt(value.toString());
                    } catch (NumberFormatException e) {
                        LOG.error("Error converting field {} to int: {}", fieldName, value, e);
                        return 0; // default value on conversion error
                    }
                }
            }

            public String getMainCast(String cast) {
                if (cast == null || cast.isEmpty()) {
                    return "";
                }
                return cast.split(",")[0];
            }

            private String convertTimestamp(long timestamp) {
                Instant instant = Instant.ofEpochMilli(timestamp);
                LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return dateTime.format(formatter);
            }

        }));
    }

}
