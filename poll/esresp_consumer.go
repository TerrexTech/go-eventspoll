package poll

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/TerrexTech/uuuid"

	"github.com/Shopify/sarama"
	ec "github.com/TerrexTech/go-eventspoll/errors"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/pkg/errors"
)

// EventResponse is the response distributed over the EventsIO channels.
type EventResponse struct {
	Event model.Event
	Error error
}

// esRespHandler handler for Consumer Messages
type esRespHandler struct {
	eventsIO   *EventsIO
	readConfig ReadConfig
}

func (*esRespHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Kafka ESQueryRespConsumer")
	return nil
}

func (e *esRespHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka ESQueryRespConsumer")
	return nil
}

func (e *esRespHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("Listening for EventStoreQuery-Responses...")
	for msg := range claim.Messages() {
		go func(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
			kr := &model.KafkaResponse{}
			err := json.Unmarshal(msg.Value, kr)
			if err != nil {
				err = errors.Wrap(err, "Error Unmarshalling ESQuery-Response into KafkaResponse")
				log.Println(err)
				return
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
				return
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
						session.MarkMessage(msg, "")
						e.eventsIO.delete <- eventResp
					}
				case "insert":
					if e.readConfig.EnableInsert {
						session.MarkMessage(msg, "")
						e.eventsIO.insert <- eventResp
					}
				case "query":
					if e.readConfig.EnableQuery {
						session.MarkMessage(msg, "")
						e.eventsIO.query <- eventResp
					}
				case "update":
					if e.readConfig.EnableUpdate {
						session.MarkMessage(msg, "")
						e.eventsIO.update <- eventResp
					}
				default:
					log.Printf("Invalid Action found in Event %s", event.TimeUUID)
					session.MarkMessage(msg, "")
				}
			}
		}(session, msg)
	}
	return nil
}
