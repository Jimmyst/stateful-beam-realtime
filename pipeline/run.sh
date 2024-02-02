#source ~/.env;
mvn compile exec:java -Dexec.mainClass=org.stjimmy.beam.LtvPipeline -Dexec.args="--inputSubscription=projects/staging-204519/subscriptions/transactions-input-sub --outputTopic=projects/name/topics/transactions-output --transactionsTable=project.ods.transactions --tempLocation=gs://bucket --project=project" -Pdirect-runner
