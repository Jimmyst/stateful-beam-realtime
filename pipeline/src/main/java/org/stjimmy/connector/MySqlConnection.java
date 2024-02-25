package org.stjimmy.connector;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.stjimmy.options.LtvPipelineSqlLookupOptions;

public class MySqlConnection {

    public static JdbcIO.DataSourceConfiguration init(LtvPipelineSqlLookupOptions options) {
        return JdbcIO.DataSourceConfiguration.create(
            "com.mysql.jdbc.Driver",
                        "jdbc:mysql://google/" + options.getCloudSqlDb()+
                                "?cloudSqlInstance=" + options.getCloudSqlInstanceConnectionName() +
                                "&socketFactory=com.google.cloud.sql.mysql.SocketFactory" +
                                "&useUnicode=true&characterEncoding=UTF-8&connectionCollation=utf8mb4_unicode_ci")
                .withUsername(options.getCloudSqlUsername())
                
                .withPassword(options.getCloudSqlPassword());

    }
    
}


// package org.stjimmy.connector;

// import org.apache.beam.sdk.io.jdbc.JdbcIO;
// import org.stjimmy.options.LtvPipelineSqlLookupOptions;

// public class MySqlConnector {

//     public static JdbcIO.DataSourceConfiguration init(LtvPipelineSqlLookupOptions options)

//     final JdbcIO.DataSourceConfiguration configurationMySQL = JdbcIO.DataSourceConfiguration.create(
//         "com.mysql.jdbc.Driver",
//         "jdbc:mysql://google/%s?cloudSqlInstance=%s".format(options.getCloudSqlDb(), options.getCloudSqlDb()) +
//             "&socketFactory=com.google.cloud.sql.mysql.SocketFactory" +
//             "&useUnicode=true&characterEncoding=UTF-8&connectionCollation=utf8mb4_unicode_ci")
//         .withUsername(options.getCloudSqlUsername())
        
//         .withPassword(options.getCloudSqlPassword());

//        DataSource ds = configurationMySQL.buildDatasource();

    
// }

