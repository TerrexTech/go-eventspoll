package examples

import (
	"log"

	"github.com/TerrexTech/go-eventspoll/poll"
	"github.com/TerrexTech/go-eventstore-models/model"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

type item struct {
	ID         objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Word       string            `bson:"word,omitempty" json:"word,omitempty"`
	Definition string            `bson:"definition,omitempty" json:"definition,omitempty"`
	Hits       int               `bson:"hits,omitempty" json:"hits,omitempty"`
}

func createMongoConnection() (*mongo.ConnectionConfig, error) {
	mongoConfig := mongo.ClientConfig{
		Hosts:               []string{"localhost:27017"},
		Username:            "root",
		Password:            "root",
		TimeoutMilliseconds: 5000,
	}

	// ====> MongoDB Client
	client, err := mongo.NewClient(mongoConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating MongoClient")
		return nil, err
	}

	conn := &mongo.ConnectionConfig{
		Client:  client,
		Timeout: 3000,
	}
	return conn, nil
}

// createMongoCollection simulates creating a basic MongoCollection.
func createMongoCollection(conn *mongo.ConnectionConfig) (*mongo.Collection, error) {
	// Index Configuration
	indexConfigs := []mongo.IndexConfig{
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name:        "word",
					IsDescOrder: true,
				},
			},
			IsUnique: true,
			Name:     "test_index",
		},
	}

	// ====> Create New Collection
	c := &mongo.Collection{
		Connection:   conn,
		Name:         "test_coll",
		Database:     "test",
		SchemaStruct: &item{},
		Indexes:      indexConfigs,
	}
	collection, err := mongo.EnsureCollection(c)
	if err != nil {
		err = errors.Wrap(err, "Error creating MongoCollection")
		return nil, err
	}
	return collection, nil
}

func main() {
	conn, err := createMongoConnection()
	if err != nil {
		err = errors.Wrap(err, "Error creating MongoConnection")
		log.Fatalln(err)
	}

	collection, err := createMongoCollection(conn)
	if err != nil {
		err = errors.Wrap(err, "Error creating MongoCollection")
		log.Fatalln(err)
	}

	kc := poll.KafkaConfig{
		Brokers: []string{"kafka:9092"},

		ConsumerEventGroup:      "my-service.consumer.group",
		ConsumerEventQueryGroup: "my-service.esquery.consumer.group",

		ConsumerEventTopic:      "event.rns_eventstore.events",
		ConsumerEventQueryTopic: "events.rns_eventstore.esresponse.2",
		ProducerEventQueryTopic: "events.rns_eventstore.esquery",
		ProducerResponseTopic:   "resp",
	}
	mc := poll.MongoConfig{
		AggregateID:        2,
		AggCollection:      collection,
		Connection:         conn,
		MetaDatabaseName:   "rns_projections",
		MetaCollectionName: "aggregate_meta",
	}
	ioConfig := poll.IOConfig{
		// Choose what type of events we need process
		// Remember, adding a type here and not processing/listening to it will cause deadlocks!
		ReadConfig: poll.ReadConfig{
			EnableInsert: true,
			EnableUpdate: true,
		},
		KafkaConfig: kc,
		MongoConfig: mc,
	}

	eventPoll, err := poll.Init(ioConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating EventPoll service")
		log.Fatalln(err)
	}

	// Handle poll errors
	go func() {
		cancelCtx := *eventPoll.CancelCtx()
		<-cancelCtx.Done()
		log.Fatalln("A critical error occurred, service will now exit")
	}()

	go func() {
		// Handle Insert events
		for eventResp := range eventPoll.Insert() {
			kafkaResp := handleInsert(eventResp)
			eventPoll.ProduceResult() <- kafkaResp
		}
	}()

	// Block the main thread from exiting using one of the channels
	// Handle Update events
	for eventResp := range eventPoll.Update() {
		kafkaResp := handleUpdate(eventResp)
		eventPoll.ProduceResult() <- kafkaResp
	}
}

func handleInsert(eventResp *poll.EventResponse) *model.KafkaResponse {
	err := eventResp.Error
	if err != nil {
		err = errors.Wrap(err, "Some error occurred")
		log.Println(err)
		// Would ideally do proper error handling as required
		return nil
	}

	event := eventResp.Event
	// Do something with Event/Event-Data
	log.Printf("%+v", event)

	// The response/result from this service, to be used by other services requesting it.
	kr := &model.KafkaResponse{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		Result:        []byte("some_data-result-of-processing-this-event"),
	}
	return kr
}

func handleUpdate(eventResp *poll.EventResponse) *model.KafkaResponse {
	err := eventResp.Error
	if err != nil {
		err = errors.Wrap(err, "Some error occurred")
		log.Println(err)
		// Would ideally do proper error handling as required
		return nil
	}

	event := eventResp.Event
	// Do something with Event/Event-Data
	log.Printf("%+v", event)

	// The response/result from this service, to be used by other services requesting it.
	kr := &model.KafkaResponse{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		Result:        []byte("some_data-result-of-processing-this-event"),
	}
	return kr
}
