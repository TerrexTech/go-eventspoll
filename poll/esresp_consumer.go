package poll

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	ec "github.com/TerrexTech/go-eventspoll/errors"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// EventResponse is the response distributed over the EventsIO channels.
type EventResponse struct {
	Event model.Event
	Error error
}

// esRespHandler handler for Consumer Messages
type esRespHandler struct {
	aggID          int8
	eventsIO       *EventsIO
	readConfig     ReadConfig
	metaCollection *mongo.Collection
}

func (*esRespHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Kafka ESQueryRespConsumer")
	return nil
}

func (*esRespHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka ESQueryRespConsumer")
	return nil
}

func (e *esRespHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("Listening for EventStoreQuery-Responses...")

	// Buffered, in case writing to DB takes longer than expected
	versionChan := make(chan int64, 100)
	updateAggMeta(
		session.Context(),
		e.aggID,
		e.metaCollection,
		(<-chan int64)(versionChan),
	)

	for msg := range claim.Messages() {
		kr := &model.KafkaResponse{}
		err := json.Unmarshal(msg.Value, kr)
		if err != nil {
			err = errors.Wrap(err, "Error Unmarshalling ESQuery-Response into KafkaResponse")
			log.Println(err)
			continue
		}

		var krError error
		if kr.Error != "" {
			krError = fmt.Errorf("Error %d: %s", kr.ErrorCode, kr.Error)
		}

		// Get all events from KafkaResponse
		events := &[]model.Event{}
		err = json.Unmarshal(kr.Result, events)
		if err != nil {
			err = errors.Wrap(err, "Error Unmarshalling KafkaResult into Events")
			log.Println(err)

			uuid, uerr := uuuid.NewV4()
			if uerr != nil {
				uerr = errors.Wrap(uerr, "Error getting UUID for KafkaResponse")
				uuid = uuuid.UUID{}
			}
			e.eventsIO.ProduceResult() <- &model.KafkaResponse{
				AggregateID:   kr.AggregateID,
				CorrelationID: kr.CorrelationID,
				Error:         err.Error(),
				ErrorCode:     ec.InternalError,
				UUID:          uuid,
			}
			continue
		}

		// Distribute events to their respective channels
		for _, event := range *events {
			eventResp := &EventResponse{
				Event: event,
				Error: krError,
			}

			switch event.Action {
			case "delete":
				if e.readConfig.EnableDelete {
					e.eventsIO.delete <- eventResp
					session.MarkMessage(msg, "")
					versionChan <- event.Version
				}
			case "insert":
				if e.readConfig.EnableInsert {
					e.eventsIO.insert <- eventResp
					session.MarkMessage(msg, "")
					versionChan <- event.Version
				}
			case "query":
				if e.readConfig.EnableQuery {
					// We can give user error message if query fails,
					// and we don't want query-messages to accumulate if some service fails,
					// so we mark them here
					session.MarkMessage(msg, "")
					e.eventsIO.query <- eventResp
					versionChan <- event.Version
				}
			case "update":
				if e.readConfig.EnableUpdate {
					e.eventsIO.update <- eventResp
					session.MarkMessage(msg, "")
					versionChan <- event.Version
				}
			default:
				log.Printf("Invalid Action found in Event %s", event.TimeUUID)
				session.MarkMessage(msg, "")
			}
		}
	}
	return nil
}

func updateAggMeta(
	ctx context.Context,
	aggID int8,
	coll *mongo.Collection,
	versionChan <-chan int64,
) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("--> Closed Aggregate meta-update routine")
				return
			case version := <-versionChan:
				filter := &AggregateMeta{
					AggregateID: aggID,
				}
				update := map[string]int64{
					"version": version,
				}
				_, err := coll.UpdateMany(filter, update)
				// Non-fatal error, so we'll just print this to stdout for info, for now
				if err != nil {
					err = errors.Wrap(err, "Error updating AggregateMeta")
					log.Println(err)
				}
			}
		}
	}()
}
