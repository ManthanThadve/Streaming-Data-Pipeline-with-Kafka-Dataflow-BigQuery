package mt.netflix.dataplatform.transforms;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;

import mt.netflix.dataplatform.utils.AvroSchemaGenerator;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;

public class EnrichGenericRecordsFn extends DoFn<KafkaRecord<String, GenericRecord>, KV<String, GenericRecord>> {

    String enrichedSchemaPath;
    public EnrichGenericRecordsFn(String enrichedSchemaPath){
        this.enrichedSchemaPath = enrichedSchemaPath;
    }

    @ProcessElement
    public void processElement(@Element KafkaRecord<String, GenericRecord> element, OutputReceiver<KV<String, GenericRecord>> out) throws IOException {


        long timestamp = element.getTimestamp();
        int partition = element.getPartition();
        long offset = element.getOffset();
        String topic = element.getTopic();

        GenericRecord existingRecord = element.getKV().getValue();
        String key = element.getKV().getKey();

        Schema modifiedSchema = AvroSchemaGenerator
                .generateAvroSchema(enrichedSchemaPath);

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

        out.output(KV.of(key, enrichedRecord));

    }
}
