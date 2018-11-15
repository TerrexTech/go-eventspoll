package poll

import (
	"context"
	"encoding/json"
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type esQueryReqProdConfig struct {
	g        *errgroup.Group
	closeCtx context.Context

	aggID           int8
	mongoColl       *mongo.Collection
	kafkaProdConfig *kafka.ProducerConfig
	kafkaTopic      string
	eventRespChan   <-chan model.Document
}

// esQueryReqProducer produces the EventStoreQuery requests to get new events.
// This is triggered everytime an event is produced.
func esQueryReqProducer(config *esQueryReqProdConfig) error {
	p, err := kafka.NewProducer(config.kafkaProdConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating ESQueryRequest-Producer")
		return err
	}

	config.g.Go(func() error {
		var prodErr error
	errLoop:
		for {
			select {
			case <-config.closeCtx.Done():
				prodErr = errors.New("ESQueryRequest-Producer: session closed")
				break errLoop
			case err := <-p.Errors():
				if err != nil && err.Err != nil {
					parsedErr := errors.Wrap(err.Err, "Error in ESQueryRequest-Producer")
					log.Println(parsedErr)
					log.Println(err)
				}
			}
		}
		log.Println("--> Closed ESQueryRequest-Producer error-routine")
		return prodErr
	})

	closeProducer := false
	config.g.Go(func() error {
		for {
			select {
			case <-config.closeCtx.Done():
				closeProducer = true
				p.Close()
				log.Println("--> Closed ESQueryRequest-Producer")
				return errors.New("ESQueryRequest-Producer: exited")
			// Replace provided version with MaxVersion from DB and send the query to
			// EventStoreQuery service.
			case doc := <-config.eventRespChan:
				currVersion, err := getVersion(config.aggID, config.mongoColl)
				if err != nil {
					err = errors.Wrapf(
						err,
						"Error fetching max version for AggregateID %d",
						doc.AggregateID,
					)
					log.Println(err)
					continue
				}

				// Create EventStoreQuery
				esQuery := model.EventStoreQuery{
					AggregateID:      doc.AggregateID,
					AggregateVersion: currVersion,
					CorrelationID:    doc.CorrelationID,
					YearBucket:       2018,
					UUID:             doc.UUID,
				}
				esMsg, err := json.Marshal(esQuery)
				if err != nil {
					err = errors.Wrap(err, "ESQueryRequest-Producer: Error Marshalling EventStoreQuery")
					log.Println(err)
					continue
				}
				msg := kafka.CreateMessage(config.kafkaTopic, esMsg)

				if !closeProducer {
					p.Input() <- msg
				} else {
					log.Println("Closed producer before producing ESQueryRequest")
				}
			}
		}
	})
	return nil
}
