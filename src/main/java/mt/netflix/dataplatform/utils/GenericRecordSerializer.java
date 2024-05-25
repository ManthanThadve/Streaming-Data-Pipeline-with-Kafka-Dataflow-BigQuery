package mt.netflix.dataplatform.utils;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GenericRecordSerializer implements Serializer<GenericRecord> {

    private final KafkaAvroSerializer inner;

    public GenericRecordSerializer() {
        inner = new KafkaAvroSerializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, GenericRecord data) {
        return inner.serialize(topic, data);
    }

    @Override
    public void close() {
        inner.close();
    }
}
