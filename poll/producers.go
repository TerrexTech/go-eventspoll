package poll

import (
	ctx "context"
	"encoding/json"
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/producer"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

type esQueryReqProdConfig struct {
	dbFailThreshold int16
	cancelCtx       ctx.Context
	mongoColl       *mongo.Collection
	kafkaProdConfig producer.Config
	kafkaTopic      string
	eventChan       <-chan model.Event
}

// esQueryReqProducer produces the EventStoreQuery requests to get new events.
// This is triggered everytime an event is produced.
func esQueryReqProducer(config *esQueryReqProdConfig) error {
	p, err := producer.New(&config.kafkaProdConfig)
	if err != nil {
		err = errors.Wrap(err, "ESQueryReqProducer: Error creating Kafka-Produer")
		return err
	}

	go func() {
		<-config.cancelCtx.Done()
		p.Close()
	}()

	go func() {
		for err := range p.Errors() {
			parsedErr := errors.Wrap(err.Err, "EventStoreQuery-Producer Error")
			log.Println(parsedErr)
			log.Println(err)
		}
	}()

	go func() {
		// Replace provided version with MaxVersion from DB and send the query to
		// EventStoreQuery service.
		for event := range config.eventChan {
			go func(event model.Event) {
				currVersion, err := getMaxVersion(config.mongoColl, config.dbFailThreshold)
				if err != nil {
					err = errors.Wrapf(
						err,
						"Error fetching max version for AggregateID %d",
						event.AggregateID,
					)
					return
				}

				// Create EventStoreQuery
				esQuery := model.EventStoreQuery{
					AggregateID:      event.AggregateID,
					AggregateVersion: currVersion,
					CorrelationID:    event.CorrelationID,
					YearBucket:       event.YearBucket,
				}
				esMsg, err := json.Marshal(esQuery)
				if err != nil {
					err = errors.Wrap(err, "ESQueryReqProducer: Error Marshalling EventStoreQuery")
					log.Println(err)
					return
				}
				msg := producer.CreateMessage(config.kafkaTopic, esMsg)
				inputChan, err := p.Input()
				if err != nil {
					err = errors.Wrap(
						err,
						"ESQueryReqProducer: Error while producing EventStoreQuery-Request",
					)
					log.Println(err)
					return
				}
				inputChan <- msg
			}(event)
		}
	}()
	return nil
}

// resultProducer produces the results for the events processed by this service, to be
// consumed by other services and proceed as required.
func resultProducer(
	cancelCtx ctx.Context,
	config producer.Config,
	topic string,
	resultChan <-chan *model.KafkaResponse,
) error {
	p, err := producer.New(&config)
	if err != nil {
		err = errors.Wrap(err, "ResultProducer: Error creating Kafka-Produer")
		return err
	}

	go func() {
		<-cancelCtx.Done()
		p.Close()
	}()

	go func() {
		for err := range p.Errors() {
			parsedErr := errors.Wrap(err.Err, "ResultProducer Error")
			log.Println(parsedErr)
			log.Println(err)
		}
	}()

	go func() {
		for kr := range resultChan {
			krmsg, err := json.Marshal(kr)
			if err != nil {
				err = errors.Wrap(err, "ResultProducer: Errors Marshalling KafkaResponse")
				log.Println(err)
				continue
			}

			inputChan, err := p.Input()
			if err != nil {
				err = errors.Wrap(err, "ResultProducer: Error while producing KafkaResponse")
				log.Println(err)
				continue
			}

			msg := producer.CreateMessage(topic, krmsg)
			inputChan <- msg
		}
	}()
	return nil
}
