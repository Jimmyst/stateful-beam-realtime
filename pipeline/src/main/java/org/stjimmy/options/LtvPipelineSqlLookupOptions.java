package org.stjimmy.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface LtvPipelineSqlLookupOptions extends CloudSqlOptions {

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


}