package mt.netflix.dataplatform.transforms;

import mt.netflix.dataplatform.utils.CsvRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectDatumWriter;

public class CsvRecordToGenericRecord extends DoFn<CsvRecord, GenericRecord> {

        private final ReflectDatumWriter writer;

        public CsvRecordToGenericRecord() {
            this.writer = new ReflectDatumWriter<>(CsvRecord.class);
        }

//        @ProcessElement
//        public void processElement(ProcessContext c, GenericRecord out) throws Exception {
//            writer.write(out, c.element());
//            c.output(out);
//        }
    }
