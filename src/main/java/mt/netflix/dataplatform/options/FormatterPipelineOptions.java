package mt.netflix.dataplatform.options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation;


public interface FormatterPipelineOptions extends PipelineOptions{

    @Validation.Required
    ValueProvider<String> getInputFolderPath();

    void setInputFolderPath(ValueProvider<String> inputFolderPath);

    @Validation.Required
    ValueProvider<String> getInputFileName();

    void setInputFileName(ValueProvider<String> inputFileName);

    @Validation.Required
    ValueProvider<String> getOutputFolderPath();

    void setOutputFolderPath(ValueProvider<String> outputFolderPath);

    @Validation.Required
    ValueProvider<String> getOutputFileName();

    void setOutputFileName(ValueProvider<String> outputFileName);

}
