package mt.netflix.dataplatform.options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;

public interface FormatterPipelineOptions extends PipelineOptions, SnowflakePipelineOptions {

    @Validation.Required
    ValueProvider<String> getSchemaRegistry();

    void setSchemaRegistry(ValueProvider<String> schemaRegistry);

    @Validation.Required
    ValueProvider<String> getBootstrapServer();

    void setBootstrapServer(ValueProvider<String> bootstrapServer);

    @Validation.Required
    ValueProvider<String> getInputTopic();

    void setInputTopic(ValueProvider<String> inputTopic);

    @Validation.Required
    ValueProvider<String> getRawSchemaPath();

    void setRawSchemaPath(ValueProvider<String> rawSchemaPath);

    @Validation.Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> outputTopic);

    @Validation.Required
    ValueProvider<String> getEnrichSchemaPath();

    void setEnrichSchemaPath(ValueProvider<String> enrichSchemaPath);

}

