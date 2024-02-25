package org.stjimmy.connector;

import javax.sql.DataSource;

import org.stjimmy.beam.LtvPipeline;
import org.stjimmy.options.LtvPipelineSqlLookupOptions;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlConnectionSingleton {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlConnectionSingleton.class);

    DataSource dataSource;
    private static MySqlConnectionSingleton instance = null;
    private final static Object lock = new Object();

    public static MySqlConnectionSingleton getInstance(String cloudSqlDb,
    String cloudSqlInstanceConnectionName,
    String cloudSqlUsername,
    String cloudSqlPassword ) {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new MySqlConnectionSingleton(cloudSqlDb, cloudSqlInstanceConnectionName, cloudSqlUsername,
                    cloudSqlPassword);
                }
            }
        }
        return instance;
    }

    

    private MySqlConnectionSingleton(String cloudSqlDb,
    String cloudSqlInstanceConnectionName,
    String cloudSqlUsername,
    String cloudSqlPassword) {
        this.dataSource = init(cloudSqlDb, cloudSqlInstanceConnectionName, cloudSqlUsername,
        cloudSqlPassword);
    }


    private DataSource init(String cloudSqlDb,
    String cloudSqlInstanceConnectionName,
    String cloudSqlUsername,
    String cloudSqlPassword) {
        LOG.info("DB_INIT");
        return JdbcIO.DataSourceConfiguration.create(
            "com.mysql.jdbc.Driver",
                        "jdbc:mysql://google/" + cloudSqlDb+
                                "?cloudSqlInstance=" + cloudSqlInstanceConnectionName +
                                "&socketFactory=com.google.cloud.sql.mysql.SocketFactory" +
                                "&useUnicode=true&characterEncoding=UTF-8&connectionCollation=utf8mb4_unicode_ci")
                .withUsername(cloudSqlUsername)
                .withMaxConnections(100)
                
                .withPassword(cloudSqlPassword).buildDatasource();
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    
}


// public class ConnectionPoolProvider {

//     DataSource dataSource;
//     private static ConnectionPoolProvider instance = null;
//     private final static Object lock = new Object();

//     public static ConnectionPoolProvider getInstance(String cloudSqlDb,
//             String cloudSqlInstanceConnectionName,
//             String cloudSqlUsername,
//             String cloudSqlPassword,
//             int minConnections,
//             int maxConnections) {
//         if (instance == null) {
//             synchronized (lock) {
//                 if (instance == null) {
//                     instance = new ConnectionPoolProvider(cloudSqlDb, cloudSqlInstanceConnectionName, cloudSqlUsername,
//                             cloudSqlPassword, minConnections, maxConnections);
//                 }
//             }
//         }
//         return instance;
//     }

//     private ConnectionPoolProvider(
//             String cloudSqlDb,
//             String cloudSqlInstanceConnectionName,
//             String cloudSqlUsername,
//             String cloudSqlPassword,
//             int minConnections,
//             int maxConnections) {
//         this.dataSource = getHikariDataSource(cloudSqlDb, cloudSqlInstanceConnectionName, cloudSqlUsername,
//                 cloudSqlPassword, minConnections, maxConnections);
//     }

//     private DataSource getHikariDataSource(
//             String cloudSqlDb,
//             String cloudSqlInstanceConnectionName,
//             String cloudSqlUsername,
//             String cloudSqlPassword,
//             int minConnections,
//             int maxConnections) {
//         HikariConfig config = new HikariConfig();
//         // Configure which instance and what database user to connect with.
//         config.setJdbcUrl("jdbc:mysql://google/" + cloudSqlDb);
//         config.setUsername(cloudSqlUsername);
//         config.setPassword(cloudSqlPassword);
//         config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.mysql.SocketFactory");
//         config.addDataSourceProperty("cloudSqlInstance", cloudSqlInstanceConnectionName);

//         config.addDataSourceProperty("useSSL", "false");
//         // maximumPoolSize limits the total number of concurrent connections this pool
//         // will keep.
//         config.setMaximumPoolSize(maxConnections);
//         // minimumIdle is the minimum number of idle connections Hikari maintains in the
//         // pool.
//         // Additional connections will be established to meet this value unless the pool
//         // is full.
//         config.setMinimumIdle(minConnections);
//         // setConnectionTimeout is the maximum number of milliseconds to wait for a
//         // connection checkout.
//         // Any attempt to retrieve a connection from this pool that exceeds the set
//         // limit will throw an
//         // SQLException.
//         config.setConnectionTimeout(10000); // 10 seconds
//         // idleTimeout is the maximum amount of time a connection can sit in the pool.
//         // Connections that
//         // sit idle for this many milliseconds are retried if minimumIdle is exceeded.
//         config.setIdleTimeout(600000); // 10 minutes
//         // maxLifetime is the maximum possible lifetime of a connection in the pool.
//         // Connections that
//         // live longer than this many milliseconds will be closed and reestablished
//         // between uses. This
//         // value should be several minutes shorter than the database's timeout value to
//         // avoid unexpected
//         // terminations.
//         config.setMaxLifetime(1800000); // 30 minutes

//         return new HikariDataSource(config);
//     }

//     public DataSource getDataSource() {
//         return dataSource;
//     }

// }