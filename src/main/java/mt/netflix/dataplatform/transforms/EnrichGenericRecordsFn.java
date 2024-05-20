package mt.netflix.dataplatform.transforms;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;

import mt.netflix.dataplatform.utils.AvroSchemaGenerator;

import java.io.IOException;

public class EnrichGenericRecordsFn extends DoFn<KafkaRecord<String, GenericRecord>, GenericRecord> {

    @ProcessElement
    public void processElement(ProcessContext c, KafkaRecord<String, GenericRecord> record) throws IOException {

        long timestamp = record.getTimestamp();
        int partition = record.getPartition();
        long offset = record.getOffset();
        String topic = record.getTopic();

        GenericRecord existingRecord = record.getKV().getValue();

        Schema modifiedSchema = AvroSchemaGenerator.generateAvroSchema();

        assert existingRecord != null;
        GenericRecord enrichedRecord = new GenericRecordBuilder(modifiedSchema)
                .set("title", existingRecord.get("title"))
                .set("cast", existingRecord.get("cast"))
                .set("country", existingRecord.get("country"))
                .set("date_added", existingRecord.get("date_added"))
                .set("release_year", existingRecord.get("release_year"))
                .set("rating", existingRecord.get("rating"))
                .set("duration", existingRecord.get("duration"))
                .set("listed_in", existingRecord.get("listed_in"))
                .set("description", existingRecord.get("description"))
                .set("number_of_cast", existingRecord.get("number_of_cast"))
                .set("timestamp", timestamp)
                .set("source_topic", topic)
                .set("partition", partition)
                .set("offset", offset)
                .build();

        c.output(enrichedRecord);

    }
}
