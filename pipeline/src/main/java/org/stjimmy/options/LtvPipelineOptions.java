package org.stjimmy.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface LtvPipelineOptions extends StreamingOptions {

    /**
     * PubSub supscription topic
     */

    @Required
    @Description("PubSub subcription to read")
    String getInputSubscription();

    void setInputSubscription(String value);

    @Required
    @Description("PubSub topic to push")
    String getOutputTopic();

    void setOutputTopic(String value);

    @Required
    @Description("Transactions table")
    String getTransactionsTable();

    void setTransactionsTable(String value);

    /** Set this required option to specify where to write the output. */
    // @Description("Path of the file to write to")
    // @Required
    // String getOutput();
    // void setOutput(String value);

    /** Set this required option to specify where to write the output. */

}