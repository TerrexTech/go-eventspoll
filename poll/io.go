package poll

import (
	"context"

	"github.com/TerrexTech/go-eventstore-models/model"
)

// EventsIO allows interacting with the EventsPoll service.
// This is the medium through which the new events are distributed.
type EventsIO struct {
	cancelFunc  *context.CancelFunc
	delete      chan *EventResponse
	insert      chan *EventResponse
	query       chan *EventResponse
	update      chan *EventResponse
	invalid     chan *EventResponse
	resultInput chan *model.KafkaResponse
}

// Close closes any open Kafka consumers/producers and other channels, such as delete,
// insert, query, update, invalid, resultInput associated with EventsPoll service.
// Use this when the service is no longer required.
func (ec *EventsIO) Close() {
	cancel := *ec.cancelFunc
	cancel()
	close(ec.resultInput)
}

// Delete is the channel for "delete" events.
func (ec *EventsIO) Delete() <-chan *EventResponse {
	return (<-chan *EventResponse)(ec.delete)
}

// Insert is the channel for "insert" events.
func (ec *EventsIO) Insert() <-chan *EventResponse {
	return (<-chan *EventResponse)(ec.insert)
}

// Query is the channel for "query" events.
func (ec *EventsIO) Query() <-chan *EventResponse {
	return (<-chan *EventResponse)(ec.query)
}

// Update is the channel for "update" events.
func (ec *EventsIO) Update() <-chan *EventResponse {
	return (<-chan *EventResponse)(ec.update)
}

// Invalid is the channel for invalid events. Invalid events are those whose "action"
// field is not one of "insert", "update", "delete" or "query".
func (ec *EventsIO) Invalid() <-chan *EventResponse {
	return (<-chan *EventResponse)(ec.invalid)
}

// ProduceResult can be used to produce the resulting Kafka-Message after/while processing
// events from EventsPoll service.
func (ec *EventsIO) ProduceResult() chan<- *model.KafkaResponse {
	return (chan<- *model.KafkaResponse)(ec.resultInput)
}
