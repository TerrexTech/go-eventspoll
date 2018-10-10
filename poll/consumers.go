package poll

import (
	ctx "context"
	"encoding/json"
	"fmt"
	"log"

	ec "github.com/TerrexTech/go-eventspoll/errors"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/pkg/errors"
)

// EventResponse is the response distributed over the EventsIO channels.
type EventResponse struct {
	Event model.Event
	Error error
}

func eventsConsumer(
	aggregateID int8,
	cancelCtx ctx.Context,
	config consumer.Config,
) (<-chan model.Event, error) {
	c, err := consumer.New(&config)
	if err != nil {
		err = errors.Wrap(err, "EventCosumer: Error creating Kafka-Consumer")
		return nil, err
	}
	eventChan := make(chan model.Event)

	// Close consumer when associated context is cancelled
	go func() {
		<-cancelCtx.Done()
		close(eventChan)
		c.Close()
	}()

	go func() {
		for cerr := range c.Errors() {
			cerr := errors.Wrap(cerr, "EventConsumer - Consumer Error")
			log.Println(cerr)
		}
	}()

	go func() {
		// FanOut messages over the eventChan
		for eventMsg := range c.Messages() {
			event := &model.Event{}
			err := json.Unmarshal(eventMsg.Value, event)
			if err != nil {
				// MarkOffset so this broken event doesn't appear again
				c.MarkOffset(eventMsg, "")
				err = errors.Wrap(err, "EventsConsumer EventHandler: Error Unmarshalling Event")
				log.Println(err)
				continue
			}
			if event.AggregateID == aggregateID {
				c.MarkOffset(eventMsg, "")
				eventChan <- *event
			}
		}
	}()
	return (<-chan model.Event)(eventChan), nil
}

// esQueryResCosumer consumes the responses/events from EventStoreQuery service.
func esQueryResCosumer(
	cancelCtx ctx.Context,
	readConfig ReadConfig,
	config consumer.Config,
	eventsIO *EventsIO,
) error {
	c, err := consumer.New(&config)
	if err != nil {
		err = errors.Wrap(err, "ESQueryResCosumer: Error creating Kafka-Consumer")
		return err
	}

	// Close consumer when associated context is cancelled
	go func() {
		<-cancelCtx.Done()
		c.Close()
	}()

	go func() {
		for cerr := range c.Errors() {
			cerr := errors.Wrap(cerr, "EventStoreQuery - Consumer Error")
			log.Println(cerr)
		}
	}()

	go func() {
	msgLoop:
		for {
			select {
			case <-cancelCtx.Done():
				close(eventsIO.delete)
				close(eventsIO.insert)
				close(eventsIO.query)
				close(eventsIO.update)
				close(eventsIO.invalid)
				break msgLoop
			case msg := <-c.Messages():
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
					eventsIO.ProduceResult() <- &model.KafkaResponse{
						AggregateID:   kr.AggregateID,
						CorrelationID: kr.CorrelationID,
						Error:         err.Error(),
						ErrorCode:     ec.InternalError,
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
						if readConfig.EnableDelete {
							c.MarkOffset(msg, "")
							eventsIO.delete <- eventResp
						}
					case "insert":
						if readConfig.EnableInsert {
							c.MarkOffset(msg, "")
							eventsIO.insert <- eventResp
						}
					case "query":
						if readConfig.EnableQuery {
							c.MarkOffset(msg, "")
							eventsIO.query <- eventResp
						}
					case "update":
						if readConfig.EnableUpdate {
							c.MarkOffset(msg, "")
							eventsIO.update <- eventResp
						}
					default:
						log.Printf("Invalid Action found in Event %s", event.UUID.String())
						c.MarkOffset(msg, "")
						if readConfig.EnableInvalid {
							eventsIO.invalid <- eventResp
						}
					}
				}
			}
		}
	}()
	return nil
}
