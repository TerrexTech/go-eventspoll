package examples

import (
	"log"

	"github.com/TerrexTech/go-kafkautils/kafka"

	"github.com/TerrexTech/go-eventspoll/poll"

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

	kafkaBrokers := []string{"kafka:9092"}
	kc := poll.KafkaConfig{
		EventCons: &kafka.ConsumerConfig{
			KafkaBrokers: kafkaBrokers,
			GroupName:    "my-service.event.consumer.group",
			Topics:       []string{"event.persistence.response"},
		},
		ESQueryResCons: &kafka.ConsumerConfig{
			KafkaBrokers: kafkaBrokers,
			GroupName:    "my-service.esquery.consumer.group",
			Topics:       []string{"esquery.response.2"},
		},

		ESQueryReqProd: &kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		},
		ESQueryReqTopic: "events.rns_eventstore.esquery",
	}
	mc := poll.MongoConfig{
		AggregateID:        2,
		AggCollection:      collection,
		Connection:         conn,
		MetaDatabaseName:   "rns_projections",
		MetaCollectionName: "aggregate_meta",
	}
	ioConfig := poll.IOConfig{
		KafkaConfig: kc,
		MongoConfig: mc,
	}

	eventPoll, err := poll.Init(ioConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating EventPoll service")
		log.Fatalln(err)
	}

	// Select-loop to process events in order
	// Be sure to implement some timeout logic so some case doesn't block forever
	for {
		select {
		// Handle errors
		case err := <-eventPoll.Wait():
			err = errors.Wrap(err, "A critical error occurred")
			log.Fatalln(err)

		// Handle Insert events
		case eventResp := <-eventPoll.Events():
			handleEvent(eventResp)
		}
	}
}

func handleEvent(eventResp *poll.EventResponse) {
	err := eventResp.Error
	if err != nil {
		err = errors.Wrap(err, "Some error occurred")
		// Would ideally do proper error handling as required
		log.Println(err)
	}

	event := eventResp.Event
	// Do something with Event/Event-Data
	log.Printf("%+v", event)
}
