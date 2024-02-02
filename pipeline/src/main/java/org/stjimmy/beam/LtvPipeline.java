
package org.stjimmy.beam;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stjimmy.model.UserState;
import org.stjimmy.model.UserTransaction;
import org.stjimmy.options.LtvPipelineOptions;
import org.stjimmy.parser.UserTransactionParser;

import com.google.api.services.bigquery.model.TableRow;

public class LtvPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(LtvPipeline.class);

  public static void main(String[] args) {
    LtvPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(LtvPipelineOptions.class);
    options.setStreaming(true);

    runLtvPipeline(options);
  }

  static void runLtvPipeline(LtvPipelineOptions options) {

    Pipeline p = Pipeline.create(options);
    LOG.info("Starting pipeline");

    PCollectionView<Map<String, UserState>> bqInitialState = p
        .apply("Read Initial State from BigQuery",
            BigQueryIO.readTableRows()
                .fromQuery("select uid, round(sum(price),2) as total, count(uid) as n from `%s` group by uid".formatted(options.getTransactionsTable()))
                .usingStandardSql())
        .apply("TableRow to UserState", ParDo.of(new DoFn<TableRow, KV<String, UserState>>() {
          @ProcessElement
          public void processElement(@Element TableRow row, OutputReceiver<KV<String, UserState>> receiver)
              throws Exception {
            // LOG.debug("inputRow: "+row.toPrettyString());
            KV<String, UserState> kv = KV.of((String) row.get("uid"), new UserState((Double) row.get("total"), Long.parseLong(row.get("n").toString())) );
            
            receiver.output(kv);
          }
        }))
        .apply(View.asMap());

    p
        .apply("Read raw PubSub messages", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))

        .apply("Json to Transaction transformation", ParDo.of(new DoFn<String, KV<String, UserTransaction>>() {

          @ProcessElement
          public void processElement(@Element String element,
              OutputReceiver<KV<String, UserTransaction>> receiver) throws Exception {
            LOG.info("[INPUT MESSAGE]: " + element);
            // System.out.println("inputMessage: " + element);
            UserTransaction res = UserTransactionParser.parseJsonToTransactioin(element);
            KV<String, UserTransaction> kv = KV.of(res.getUid(), res);
            receiver.output(kv);
          }
        }))
        .apply("Update UserState", ParDo.of(new DoFn<KV<String, UserTransaction>, UserTransaction>() {
          @StateId("userState")
          private final StateSpec<ValueState<UserState>> userStateSpec = StateSpecs.value();
        
          @ProcessElement
          public void processElement(
              ProcessContext context,
              @Element KV<String, UserTransaction> kv,
              @StateId("userState") ValueState<UserState>  currentState,
              OutputReceiver<UserTransaction> receiver) throws Exception {

            Map<String, UserState> storage = context.sideInput(bqInitialState);

            Double newTotal;
            Long newCount;
            UserState oldStat = currentState.read();
            if (oldStat != null) {
              
              newTotal = oldStat.getTotal() + kv.getValue().getPrice();
              newCount = oldStat.getCount() + 1;

            } else {
             
              UserState storegeStat = storage.get(kv.getKey());
              
              if (storegeStat != null) {
                LOG.info("[GET FROM INITIAL STATE][%s]: %s".formatted(kv.getKey(),storegeStat.toJosn()));
                newTotal = storegeStat.getTotal() + kv.getValue().getPrice();
                newCount = storegeStat.getCount() + 1;
              } else {
                newTotal = kv.getValue().getPrice();
                newCount = Long.valueOf(1);
              }
             
            }
            UserState newStat = new UserState(newTotal, newCount);
            currentState.write(newStat);

            LOG.info("[USER STATE][%s]: %s".formatted(kv.getKey(),newStat.toJosn()));

            receiver.output(kv.getValue());

          }
        }).withSideInputs(bqInitialState)

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
