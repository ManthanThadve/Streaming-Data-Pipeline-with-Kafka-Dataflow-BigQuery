package mt.netflix.dataplatform.utils;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

public class AvroSchemaGenerator {

    private AvroSchemaGenerator(){}

    public static Schema generateAvroSchema(String filePath) throws IOException {
        Schema avroSchema;
        File schemaFile = new File(filePath);
        Schema.Parser parser = new Schema.Parser();
        avroSchema = parser.parse(schemaFile);
        return avroSchema;
    }
}
