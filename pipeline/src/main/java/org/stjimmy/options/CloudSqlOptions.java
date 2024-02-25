package org.stjimmy.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;

public interface CloudSqlOptions extends StreamingOptions {

    @Description("cloudSqlInstanceConnectionName")
    String getCloudSqlInstanceConnectionName();
    void setCloudSqlInstanceConnectionName(String value);

    @Description("cloudSqlPassword")
    String getCloudSqlPassword();
    void setCloudSqlPassword(String value);

    @Description("cloudSqlDb")
    String getCloudSqlDb();
    void setCloudSqlDb(String value);

    @Description("cloudSqlUsername")
    String getCloudSqlUsername();
    void setCloudSqlUsername(String value);


    @Description("cloudSqlTable")
    String getCloudSqlTable();
    void setCloudSqlTable(String value);
}
