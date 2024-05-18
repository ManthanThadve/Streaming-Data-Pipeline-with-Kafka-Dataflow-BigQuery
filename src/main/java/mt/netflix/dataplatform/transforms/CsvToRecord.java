package mt.netflix.dataplatform.transforms;

import mt.netflix.dataplatform.utils.CsvRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class CsvToRecord extends DoFn<String, CsvRecord> {

    private final String[] headers;
    private final TupleTag<CsvRecord> validRecords;
    private final TupleTag<String> notValidRecords;

    public CsvToRecord(String csvSchema, TupleTag<CsvRecord> validRecords, TupleTag<String> notValidRecords) {
        this.headers = csvSchema.split(",");
        this.validRecords = validRecords;
        this.notValidRecords =  notValidRecords;
    }

//    String headers = "title,country,date_added,release_year,rating,duration,category,description";
    @ProcessElement
    public void processElement(@Element String line, MultiOutputReceiver out) {
        String[] values = line.split(",");
        CsvRecord record = new CsvRecord();
        try {
            record.title = removeQuotes(values[indexOf(headers, "title")]);
            record.country = removeQuotes(values[indexOf(headers, "country")]);
            record.date_added = removeQuotes(values[indexOf(headers, "date_added")]);
            record.release_year = Integer.parseInt(removeQuotes(values[indexOf(headers, "release_year")]));
            record.rating = removeQuotes(values[indexOf(headers, "rating")]);
            record.duration = removeQuotes(values[indexOf(headers, "duration")]);
            record.category = removeQuotes(values[indexOf(headers, "category")]);
            record.description = removeQuotes(values[indexOf(headers, "description")]);
            out.get(validRecords).output(record);
        }
        catch (NumberFormatException ignored){
            out.get(notValidRecords).output(line);
            System.out.println("Error Occurred :- " + line);
        }
    }

    private int indexOf(String[] array, String target) {
        for (int i = 0; i < array.length; i++) {
            if (array[i].equals(target)) {
                return i;
            }
        }
        throw new RuntimeException("Header not found: " + target);
    }

    private String removeQuotes(String value) {
        return value.replaceAll("\"", "");
    }

}
