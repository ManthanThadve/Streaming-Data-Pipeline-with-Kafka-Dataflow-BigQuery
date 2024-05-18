package mt.netflix.dataplatform;

import mt.netflix.dataplatform.options.FormatterPipelineOptions;
import mt.netflix.dataplatform.transforms.CsvToRecord;
import mt.netflix.dataplatform.transforms.PrepareCsvData;
import mt.netflix.dataplatform.transforms.ParseCsvData;
import mt.netflix.dataplatform.utils.CsvRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class FormatterPipeline {

    public static final Logger LOGGER = LoggerFactory.getLogger(FormatterPipeline.class);

//    private static final Logger logger
//            = LoggerFactory.getLogger(Example.class);

    public static void main(String[] args) {

        FormatterPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as
                (FormatterPipelineOptions.class);

        String startMessage = String.format("Started Reading data from '%s' and write to '%s' at time %s", options.getInputFolderPath(), options.getOutputFolderPath(), new Timestamp(System.currentTimeMillis()));

        LOGGER.info(startMessage);
        run(options);

    }

    public static void run(FormatterPipelineOptions options){

        String headers = "title,country,date_added,release_year,rating,duration,category,description";

        Pipeline p = Pipeline.create(options);

        TupleTag<CsvRecord> validRecords = new TupleTag<CsvRecord>() {};
        TupleTag<String> notValidRecords = new TupleTag<String>() {};

        LOGGER.info("******************** Reading of CSV file started *********************");
        LOGGER.info("******************** Parsing of CSV data started *********************");
        PCollection<String[]> csv_data = p.apply("Read CSV Data", TextIO.read().from("M:\\Training\\Beam\\Datasets\\Netflix\\netflix-shows-500.csv"))
//        PCollection<String> raw_data = p.apply("Read CSV Data", TextIO.read().from(options.getInputFolderPath() + options.getInputFileName()))
                .apply(ParDo.of(new ParseCsvData()));


        LOGGER.info("******************** Preparing of CSV data into POJO Started *********************");
        PCollection<String> tranform_csv_data = csv_data.apply(ParDo.of(new PrepareCsvData()));

        PCollectionTuple csvRecords = tranform_csv_data.apply(ParDo.of(new CsvToRecord(headers,validRecords,notValidRecords))
                .withOutputTags(validRecords, TupleTagList.of(notValidRecords)));

        LOGGER.info("******************** Writing of Netflix records into avro file started *********************");

        csvRecords.get(validRecords).apply("write to avro file", AvroIO.write(CsvRecord.class)
                .to(options.getOutputFolderPath())
                .withShardNameTemplate(options.getOutputFileName()+ "-SSSSS-of-NNNNN.avsc").withNumShards(1));

        csvRecords.get(notValidRecords).apply(TextIO.write().to(options.getOutputFolderPath())
                .withShardNameTemplate("error-records"+ ".csv").withNumShards(1));


//        tranform_csv_data.apply(TextIO.write().to(options.getOutputFolderPath()).withShardNameTemplate(options.getOutputFileName()+ "-SSSSS-of-NNNNN.csv"));

        p.run(options);
//        System.exit(0);

    }
}
