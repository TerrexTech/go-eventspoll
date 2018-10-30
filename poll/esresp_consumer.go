package poll

import (
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

	versionChan chan int64
}

func (e *esRespHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing ESQueryResponse-Consumer")
	go updateAggMeta(e.aggID, e.metaCollection, (<-chan int64)(e.versionChan))

	return nil
}

func (e *esRespHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing ESQueryRespConsumer")

	close(e.eventsIO.delete)
	close(e.eventsIO.insert)
	close(e.eventsIO.query)
	close(e.eventsIO.update)
	close(e.versionChan)

	return errors.New("ESQueryResponse-Consumer unexpectedly closed")
}

func (e *esRespHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("ESQueryResponse-Consumer Listening...")
	for {
		select {
		case <-session.Context().Done():
			return errors.New("ESQueryResponse-Consumer: session closed")

		case msg := <-claim.Messages():
			kr := &model.KafkaResponse{}
			err := json.Unmarshal(msg.Value, kr)
			if err != nil {
				err = errors.Wrap(err, "Error Unmarshalling ESQueryResponse into KafkaResponse")
				log.Println(err)
				continue
			}
			log.Printf("Received ESQueryResponse with ID: %s", kr.UUID)

			var krError error
			if kr.Error != "" {
				krError = fmt.Errorf("Error %d: %s", kr.ErrorCode, kr.Error)
			}

			// Get all events from KafkaResponse
			events := &[]model.Event{}
			err = json.Unmarshal(kr.Result, events)
			if err != nil {
				err = errors.Wrap(
					err,
					"ESQueryResponse-Consumer: Error Unmarshalling KafkaResult into Events",
				)
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
				if event.TimeUUID == (uuuid.UUID{}) {
					continue
				}
				eventResp := &EventResponse{
					Event: event,
					Error: krError,
				}

				switch event.Action {
				case "delete":
					if e.readConfig.EnableDelete {
						e.eventsIO.delete <- eventResp
						session.MarkMessage(msg, "")
						e.versionChan <- event.Version
					}
				case "insert":
					if e.readConfig.EnableInsert {
						e.eventsIO.insert <- eventResp
						session.MarkMessage(msg, "")
						e.versionChan <- event.Version
					}
				case "query":
					if e.readConfig.EnableQuery {
						session.MarkMessage(msg, "")
						e.eventsIO.query <- eventResp
						e.versionChan <- event.Version
					}
				case "update":
					if e.readConfig.EnableUpdate {
						e.eventsIO.update <- eventResp
						session.MarkMessage(msg, "")
						e.versionChan <- event.Version
					}
				default:
					log.Printf("Invalid Action found in Event %s", event.TimeUUID)
					session.MarkMessage(msg, "")
				}
			}
		}
	}
}

func updateAggMeta(aggID int8, coll *mongo.Collection, versionChan <-chan int64) {
	for version := range versionChan {
		if version == 0 {
			continue
		}
		filter := &AggregateMeta{
			AggregateID: aggID,
		}
		update := map[string]int64{
			"version": version,
		}
		_, err := coll.UpdateMany(filter, update)
		// Non-fatal error, so we'll just print this to stdout for info, for now
		// This is Non-fatal because even if the Aggregate's version doesn't update, it'll only
		// mean that more events will have to be hydrated, which is more acceptable than
		// having the service exit.
		// TODO: Maybe define a limit on number of events?
		if err != nil {
			err = errors.Wrap(err, "Error updating AggregateMeta")
			log.Println(err)
		}
	}
	log.Println("--> Closed Aggregate meta-update routine")
}
