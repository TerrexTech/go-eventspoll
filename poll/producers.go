package poll

import (
	ctx "context"
	"encoding/json"
	"log"
	"sync"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

type esQueryReqProdConfig struct {
	aggID           int8
	cancelCtx       *ctx.Context
	mongoColl       *mongo.Collection
	kafkaProdConfig kafka.ProducerConfig
	kafkaTopic      string
	eventRespChan   <-chan model.KafkaResponse
}

// esQueryReqProducer produces the EventStoreQuery requests to get new events.
// This is triggered everytime an event is produced.
func esQueryReqProducer(config *esQueryReqProdConfig) error {
	p, err := kafka.NewProducer(&config.kafkaProdConfig)
	if err != nil {
		err = errors.Wrap(err, "ESQueryReqProducer: Error creating Kafka-Produer")
		return err
	}

	go func() {
		for err := range p.Errors() {
			parsedErr := errors.Wrap(err.Err, "EventStoreQuery-Producer Error")
			log.Println(parsedErr)
			log.Println(err)
		}
	}()

	var closeLock sync.RWMutex
	closeProducer := false
	go func() {
		for {
			select {
			case <-(*config.cancelCtx).Done():
				closeLock.Lock()
				closeProducer = true
				p.Close()
				closeLock.Unlock()
				log.Println("--> Closed ESQueryRequest-Producer")
				return
			// Replace provided version with MaxVersion from DB and send the query to
			// EventStoreQuery service.
			case kr := <-config.eventRespChan:
				go func(kr model.KafkaResponse) {
					currVersion, err := getVersion(config.aggID, config.mongoColl)
					if err != nil {
						err = errors.Wrapf(
							err,
							"Error fetching max version for AggregateID %d",
							kr.AggregateID,
						)
						log.Println(err)
						return
					}

					// Create EventStoreQuery
					esQuery := model.EventStoreQuery{
						AggregateID:      kr.AggregateID,
						AggregateVersion: currVersion,
						CorrelationID:    kr.CorrelationID,
						YearBucket:       2018,
						UUID:             kr.UUID,
					}
					esMsg, err := json.Marshal(esQuery)
					if err != nil {
						err = errors.Wrap(err, "ESQueryReqProducer: Error Marshalling EventStoreQuery")
						log.Println(err)
						return
					}
					msg := kafka.CreateMessage(config.kafkaTopic, esMsg)

					closeLock.Lock()
					if !closeProducer {
						p.Input() <- msg
					} else {
						log.Println("Closed producer before producing ESQuery-Request")
					}
					closeLock.Unlock()
				}(kr)
			}
		}
	}()
	return nil
}

// resultProducer produces the results for the events processed by this service, to be
// consumed by other services and proceed as required.
func resultProducer(
	cancelCtx *ctx.Context,
	config kafka.ProducerConfig,
	topic string,
	resultChan <-chan *model.KafkaResponse,
) error {
	p, err := kafka.NewProducer(&config)
	if err != nil {
		err = errors.Wrap(err, "ResultProducer: Error creating Kafka-Produer")
		return err
	}

	go func() {
		for err := range p.Errors() {
			parsedErr := errors.Wrap(err.Err, "ResultProducer Error")
			log.Println(parsedErr)
			log.Println(err)
		}
	}()

	var closeLock sync.RWMutex
	closeProducer := false
	go func() {
		for {
			select {
			case <-(*cancelCtx).Done():
				closeLock.Lock()
				closeProducer = true
				p.Close()
				closeLock.Unlock()
				log.Println("--> Closed ESQueryRequest-Producer")
				return

			case kr := <-resultChan:
				krmsg, err := json.Marshal(kr)
				if err != nil {
					err = errors.Wrap(err, "ResultProducer: Errors Marshalling KafkaResponse")
					log.Println(err)
					continue
				}

				msg := kafka.CreateMessage(topic, krmsg)
				closeLock.Lock()
				if !closeProducer {
					p.Input() <- msg
				} else {
					log.Println("Closed producer before producing result")
				}
				closeLock.Unlock()
			}
		}
	}()
	return nil
}
