
package org.stjimmy.beam;

import java.sql.ResultSet;
import java.util.Map;

import javax.sql.DataSource;
import javax.sql.ConnectionEvent;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stjimmy.connector.MySqlConnection;
import org.stjimmy.connector.MySqlConnectionSingleton;
import org.stjimmy.model.UserState;
import org.stjimmy.model.UserTransaction;
import org.stjimmy.options.LtvPipelineOptions;
import org.stjimmy.options.LtvPipelineSqlLookupOptions;
import org.stjimmy.parser.RowParserHelper;
import org.stjimmy.parser.UserTransactionParser;

import com.google.api.services.bigquery.model.TableRow;


public class LtvPipelineSqlLookup {

  private static final Logger LOG = LoggerFactory.getLogger(LtvPipeline.class);

  public static void main(String[] args) {
    LtvPipelineSqlLookupOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(LtvPipelineSqlLookupOptions.class);
    options.setStreaming(true);

    runLtvPipeline(options);
  }

  static void runLtvPipeline(LtvPipelineSqlLookupOptions options) {

    Pipeline p = Pipeline.create(options);
    LOG.info("Starting pipeline");

    options.setStreaming(true);

    PCollection<Void> initLookupTable = p
        .apply("Read Initial State from BigQuery",
            BigQueryIO.readTableRows()
                .fromQuery("select uid, round(sum(price),2) as total, count(uid) as n from `%s` where uid is not null group by uid "
                    .formatted(options.getTransactionsTable()))
                .usingStandardSql())
        .apply("Write to MySQL Lookup table", JdbcIO.<TableRow>write()
            .withDataSourceConfiguration(MySqlConnection.init(options))
            .withStatement(
                """
                    REPLACE INTO %s
                    (uid,total,cnt)
                    VALUES (?, ?, ?)
                    """
                    .formatted(options.getCloudSqlTable()))

            .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<TableRow>) (elem, query) -> {
              String uid = RowParserHelper.getStringValue(elem, "uid", false);
              double total = RowParserHelper.getDoubleValue(elem, "total");
              Long cnt = RowParserHelper.getLongValue(elem, "cnt");

              query.setString(1, uid);
              query.setDouble(2, total);
              query.setLong(3, cnt);

            }).withResults());

    p
        .apply("Read raw PubSub messages", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
        .apply(Wait.on(initLookupTable))

        .apply("Json to Transaction transformation", ParDo.of(new DoFn<String, KV<String, UserTransaction>>() {

          @ProcessElement
          public void processElement(@Element String element,
              OutputReceiver<KV<String, UserTransaction>> receiver) throws Exception {
            LOG.info("[INPUT MESSAGE]: " + element);

            UserTransaction res = UserTransactionParser.parseJsonToTransactioin(element);
            KV<String, UserTransaction> kv = KV.of(res.getUid(), res);
            receiver.output(kv);
          }
        }))
        .apply("Update UserState", ParDo.of(new DoFn<KV<String, UserTransaction>, UserTransaction>() {
          @StateId("userState")
          private final StateSpec<ValueState<UserState>> userStateSpec = StateSpecs.value();
          private DataSource dataSource;
          private String lookupDb;
          private String lookupTable;

          //metrics
          Distribution processingSqlQuery = Metrics.distribution("spend_level", "processing_time_sql_cache");
          Counter startBundleCounter = Metrics.counter("spend_level", "start_bundle_counter");

          @StartBundle
          public void initializeConnection(PipelineOptions opt) {

            LtvPipelineSqlLookupOptions myoptions = opt.as(LtvPipelineSqlLookupOptions.class);

            LOG.info("[INITIALIZE_STATE]");

            // dataSource = MySqlConnection.init(myoptions).buildDatasource();
            dataSource = MySqlConnectionSingleton.getInstance(
              myoptions.getCloudSqlDb(),
              myoptions.getCloudSqlInstanceConnectionName(),
              myoptions.getCloudSqlUsername(),
              myoptions.getCloudSqlPassword()
              ).getDataSource();


            lookupDb = myoptions.getCloudSqlDb();
            lookupTable = myoptions.getCloudSqlTable();

            startBundleCounter.inc();
            

          }

          @ProcessElement
          public void processElement(
              ProcessContext context,
              @Element KV<String, UserTransaction> kv,
              @StateId("userState") ValueState<UserState> currentState,
              OutputReceiver<UserTransaction> receiver) throws Exception {

            Double newTotal;
            Long newCount;
            UserState oldStat = currentState.read();
            String uid = kv.getKey();
            if (oldStat != null) {

              newTotal = oldStat.getTotal() + kv.getValue().getPrice();
              newCount = oldStat.getCount() + 1;

            } else {
              long sqlBefore = Instant.now().getMillis();

              //query to lookup table
              String query = "select * from %s.%s where uid=\"%s\"".formatted(lookupDb,lookupTable,uid);
              try {
                java.sql.Connection conn = dataSource.getConnection();
                ResultSet rs = conn.prepareStatement(query).executeQuery();
                if (rs.first()) {
                  LOG.info("[GET FROM LOOKUP][%s]".formatted(uid));
                  UserState stateFromSql = new UserState(rs.getDouble("total"),rs.getLong("cnt"));
                  oldStat = stateFromSql;
                } else {
                  //no such uid in lookup
                  LOG.info("[LOOKUP TABLE EMPTY][%s]".formatted(uid));
                  
                }
                conn.close();
              } catch (Throwable throwable) {
                LOG.error("[LOOKUP TABLE ERROR]: " +throwable);
                

              }
              
              long delta = Instant.now().getMillis() - sqlBefore;
              processingSqlQuery.update(delta);
            }

              if (oldStat != null) {
                LOG.info("[GET FROM STATE][%s]: %s".formatted(kv.getKey(), oldStat.toJosn()));
                newTotal = oldStat.getTotal() + kv.getValue().getPrice();
                newCount = oldStat.getCount() + 1;
              } else {
                newTotal = kv.getValue().getPrice();
                newCount = Long.valueOf(1);
              }

            
            UserState newStat = new UserState(newTotal, newCount);
            currentState.write(newStat);

            LOG.info("[USER STATE][%s]: %s".formatted(kv.getKey(), newStat.toJosn()));

            receiver.output(kv.getValue());

          }
        })

        )

        .apply("Transaction to JSON", ParDo.of(new DoFn<UserTransaction, String>() {
          @ProcessElement
          public void processElement(@Element UserTransaction element,
              OutputReceiver<String> receiver) throws Exception {
            LOG.info("Saved to Transactions");
            receiver.output(element.toJosn());
          }

        }))

        .apply("Transaction to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

    p.run().waitUntilFinish();
  }

}
