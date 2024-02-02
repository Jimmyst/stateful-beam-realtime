package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Transaction struct {
	Uid           string  `json:"uid" fake:"uid_{number:1,2000}"`
	Item          string  `json:"item" fake:"{snack}"`
	Price         float64 `json:"price" fake:"{price:1.0,20.0}"`
	TransactionId string  `json:"transaction_id" fake:"{uuid}"`
	TransactionTs int64   `json:"transaction_ts" fake:"skip"`
}

func main() {
	var messages int
	var sleep int
	var topicID, projectID string
	var workers int
	var wg sync.WaitGroup

	flag.IntVar(&messages, "msg", 10000, "Messages to send")
	flag.IntVar(&sleep, "sleep", 50, "Thread sleep between messages (milliseconds)")
	flag.StringVar(&topicID, "topic", "transactions-input", "Pub/Sub topic")
	flag.StringVar(&projectID, "project", "project-name", "Google project")
	flag.IntVar(&workers, "workers", 20, "Number of parallel workers")

	flag.Parse()

	pod := uuid.New().String()
	log.Infof("pod %s started!", pod)
	defer log.Infof("pod %s done", pod)

	envTopic := getEnv("TOPIC_ID", "nil")
	if envTopic != "nil" {
		topicID = envTopic
		log.Infof("using topic id from env variable: %s", topicID)
	}

	envProject := getEnv("PROJECT_ID", "nil")
	if envProject != "nil" {
		projectID = envProject
		log.Infof("using project id from env variable: %s", projectID)
	}

	log.Infof("sending %d messages each of %w workers to the topic %s in the project %s with latency %s milliseconds", messages, workers, topicID, projectID, sleep)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			publish(pod, messages, sleep, projectID, topicID)
		}()
	}

	wg.Wait()
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func publish(pod_uuid string, messages int, sleep int, projectID string, topicID string) {

	faker := gofakeit.NewCrypto()
	gofakeit.SetGlobalFaker(faker)

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()
	t := client.Topic(topicID)
	//set if you want to try ordering
	//t.EnableMessageOrdering = true
	var f Transaction
	worker_uuid := uuid.New().String()
	for i := 0; i < messages; i++ {

		gofakeit.Struct(&f)
		f.TransactionTs = time.Now().UnixMilli()
		msg, _ := json.Marshal(f)

		log.Infof("[%s][%v]: %s", worker_uuid, i, msg)
		result := t.Publish(ctx, &pubsub.Message{
			Data: []byte(msg),
			//set if you want to try ordering
			//OrderingKey: string(f.TransactionTs),
			// Attributes: map[string]string{
			// 	"msgid": msgid,
			// },
		})

		_, err := result.Get(ctx)
		if err != nil {
			log.Fatalf("pubsub: result.Get: %s", err)
		}
		time.Sleep(time.Duration(sleep) * time.Millisecond)
	}
	defer log.Infof("worker %s done", worker_uuid)

}
