package poll

import (
	ctx "context"
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/TerrexTech/go-kafkautils/producer"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

// ReadConfig allows choosing what type of events should be processed.
// Warning: Be sure to read the associated EventsIO channel if it is enabled here.
// Otherwise it will result in a deadlock!
type ReadConfig struct {
	EnableDelete  bool
	EnableInsert  bool
	EnableInvalid bool
	EnableQuery   bool
	EnableUpdate  bool
}

// KafkaConfig is the configuration for Kafka, such as brokers and topics.
type KafkaConfig struct {
	Brokers []string
	// Consumer Group-name for Events consumer.
	ConsumerEventGroup string
	// Consumer Group-name for EventStoreQuery response-consumer.
	ConsumerEventQueryGroup string
	// Topic on which responses from EventStoreQuery should be received.
	ConsumerEventQueryTopic string
	// Topic on which new events should be listened for.
	ConsumerEventTopic string
	// Topic on which requests to EventStoreQuery should be sent.
	ProducerEventQueryTopic string
	// Topic on which the service should produce its response/results,
	// for use by other services.
	ProducerResponseTopic string
}

// IOConfig is the configuration for EventPoll service.
type IOConfig struct {
	// Aggregate to process the events for.
	AggregateID     int8
	KafkaConfig     KafkaConfig
	MongoCollection *mongo.Collection
	// The number of times we allow DB to fail when fetching Max Aggregate-Version.
	// Check docs on poll.getMaxVersion function for more info.
	MongoFailThreshold int16
	ReadConfig         ReadConfig
}

// validateConfig validates the input config.
func validateConfig(config IOConfig) error {
	if config.AggregateID < 1 {
		return errors.New("AggregateID >0 is required, but none/invalid was specified")
	}
	if config.MongoFailThreshold < 1 {
		return errors.New(
			"MongoFailThreshold >0 is required, but none/invalid was specified",
		)
	}

	kfConfig := config.KafkaConfig
	if kfConfig.ConsumerEventGroup == "" {
		return errors.New(
			"ConsumerEventGroup is required, but none was specified",
		)
	}
	if kfConfig.ConsumerEventQueryGroup == "" {
		return errors.New(
			"ConsumerEventQueryGroup is required, but none was specified",
		)
	}
	if kfConfig.ConsumerEventQueryTopic == "" {
		return errors.New(
			"ConsumerEventQueryTopic is required, but none was specified",
		)
	}
	if kfConfig.ConsumerEventTopic == "" {
		return errors.New(
			"ConsumerEventTopic is required, but none was specified",
		)
	}
	if kfConfig.ProducerEventQueryTopic == "" {
		return errors.New(
			"ProducerEventQueryTopic is required, but none was specified",
		)
	}
	if kfConfig.ProducerResponseTopic == "" {
		return errors.New(
			"ProducerResponseTopic is required, but none was specified",
		)
	}

	rc := config.ReadConfig
	if !rc.EnableDelete &&
		!rc.EnableInsert &&
		!rc.EnableInvalid &&
		!rc.EnableQuery &&
		!rc.EnableUpdate {
		return errors.New(
			"Error in ReadConfig: Atleast one of the channels must be open/true",
		)
	}

	return nil
}

// Init initializes the EventPoll service.
func Init(config IOConfig) (*EventsIO, error) {
	log.Println("Initializing Poll Service")

	err := validateConfig(config)
	if err != nil {
		err = errors.Wrap(err, "Error validating Configuration")
		return nil, err
	}

	cancelCtx, cancel := ctx.WithCancel(ctx.Background())
	krChan := make(chan *model.KafkaResponse)
	eventsIO := &EventsIO{
		cancelFunc:  &cancel,
		resultInput: krChan,
		delete:      make(chan *EventResponse),
		insert:      make(chan *EventResponse),
		query:       make(chan *EventResponse),
		update:      make(chan *EventResponse),
		invalid:     make(chan *EventResponse),
	}

	kfConfig := config.KafkaConfig

	// Events Consumer
	log.Println("Initializing Events Consumer")
	eventConsConf := consumer.Config{
		ConsumerGroup: kfConfig.ConsumerEventGroup,
		KafkaBrokers:  kfConfig.Brokers,
		Topics:        []string{kfConfig.ConsumerEventTopic},
	}
	eventChan, err := eventsConsumer(config.AggregateID, cancelCtx, eventConsConf)
	if err != nil {
		cancel()
		return nil, err
	}

	prodConf := producer.Config{
		KafkaBrokers: kfConfig.Brokers,
	}

	// EventStoreQuery Request Producer
	log.Println("Initializing EventStoreQuery Request Producer")
	esqReqProdConfig := &esQueryReqProdConfig{
		cancelCtx:       cancelCtx,
		mongoColl:       config.MongoCollection,
		kafkaProdConfig: prodConf,
		dbFailThreshold: config.MongoFailThreshold,
		eventChan:       eventChan,
		kafkaTopic:      kfConfig.ProducerEventQueryTopic,
	}
	err = esQueryReqProducer(esqReqProdConfig)
	if err != nil {
		cancel()
		return nil, err
	}

	// EventStoreQuery Response Consumer
	log.Println("Initializing EventStoreQuery Response Consumer")
	esQueryConsConf := consumer.Config{
		ConsumerGroup: kfConfig.ConsumerEventQueryGroup,
		KafkaBrokers:  kfConfig.Brokers,
		Topics:        []string{kfConfig.ConsumerEventQueryTopic},
	}
	err = esQueryResCosumer(
		cancelCtx,
		config.ReadConfig,
		esQueryConsConf,
		eventsIO,
	)
	if err != nil {
		cancel()
		return nil, err
	}

	// Result Producer
	log.Println("Initializing Result Producer")
	err = resultProducer(
		cancelCtx,
		prodConf,
		kfConfig.ProducerResponseTopic,
		(<-chan *model.KafkaResponse)(krChan),
	)
	if err != nil {
		cancel()
		return nil, err
	}

	log.Println("Events-Poll Service Initialized")
	return eventsIO, nil
}
