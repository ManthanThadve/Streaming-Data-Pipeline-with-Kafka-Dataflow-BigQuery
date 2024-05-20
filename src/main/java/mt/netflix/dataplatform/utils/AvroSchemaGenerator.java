package mt.netflix.dataplatform.utils;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

public class AvroSchemaGenerator {

    private AvroSchemaGenerator(){}

    public static Schema generateAvroSchema() throws IOException {
        Schema avroSchema;
        File schemaFile = new File("M:\\Training\\Beam\\netflix-etl-formatter\\configurations\\schema\\enriched_netflix.avsc");
        Schema.Parser parser = new Schema.Parser();
        avroSchema = parser.parse(schemaFile);
        return avroSchema;
    }
}
