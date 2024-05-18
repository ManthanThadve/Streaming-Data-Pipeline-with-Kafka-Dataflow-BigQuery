package mt.netflix.dataplatform.transforms;

import org.apache.beam.sdk.transforms.DoFn;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

public class ParseCsvData extends DoFn<String, String[]> {

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String[]> receiver) throws IOException {

        CSVFormat csvFormat = CSVFormat.DEFAULT;

        try(CSVParser parser = new CSVParser(new StringReader(element), csvFormat)) {

            CSVRecord csvRecord = parser.iterator().next();
            String[] fields = new String[csvRecord.size()];

            for (int i = 0; i < fields.length; i++) {
                fields[i] = csvRecord.get(i);
            }
//            System.out.println(Arrays.toString(fields));
            receiver.output(fields);
        }
    }
}
