package mt.netflix.dataplatform.utils;

import org.apache.avro.io.Encoder;

import java.io.Serializable;

public class CsvRecord implements Serializable {

    public String title;
    public String country;
    public String date_added;
    public int release_year;
    public String rating;
    public String duration;
    public String category;
    public String description;

}
