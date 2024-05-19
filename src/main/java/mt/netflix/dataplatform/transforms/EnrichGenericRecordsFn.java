package mt.netflix.dataplatform.transforms;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class EnrichGenericRecordsFn extends DoFn<KafkaRecord<String, GenericRecord>, GenericRecord> {

    @ProcessElement
    public void processElement(ProcessElement c, KafkaRecord<String, GenericRecord> record) {

        long timestamp = record.getTimestamp();
        int partition = record.getPartition();
        long offset = record.getOffset();

        record.getKV();

    }


}
