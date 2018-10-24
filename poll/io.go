package poll

import (
	"context"

	"github.com/TerrexTech/go-eventstore-models/model"
)

// EventsIO allows interacting with the EventsPoll service.
// This is the medium through which the new events are distributed.
type EventsIO struct {
	cancelCtx   *context.Context
	cancelFunc  *context.CancelFunc
	delete      chan *EventResponse
	insert      chan *EventResponse
	query       chan *EventResponse
	update      chan *EventResponse
	resultInput chan *model.KafkaResponse
}

// CancelCtx returns the CancelContext which is executed when
// some Kafka Consumer or Producer closes for some reason.
// The reason can be when the EventsIO.Close function is called
// or even an error. This function should be used for error handling.
func (e *EventsIO) CancelCtx() *context.Context {
	return e.cancelCtx
}

// Close closes any open Kafka consumers/producers and other channels, such as delete,
// insert, query, update, invalid, resultInput associated with EventsPoll service.
// Use this when the service is no longer required.
func (e *EventsIO) Close() {
	close(e.resultInput)
	cancel := *e.cancelFunc
	cancel()
}

// Delete is the channel for "delete" events.
func (e *EventsIO) Delete() <-chan *EventResponse {
	return (<-chan *EventResponse)(e.delete)
}

// Insert is the channel for "insert" events.
func (e *EventsIO) Insert() <-chan *EventResponse {
	return (<-chan *EventResponse)(e.insert)
}

// Query is the channel for "query" events.
func (e *EventsIO) Query() <-chan *EventResponse {
	return (<-chan *EventResponse)(e.query)
}

// Update is the channel for "update" events.
func (e *EventsIO) Update() <-chan *EventResponse {
	return (<-chan *EventResponse)(e.update)
}

// ProduceResult can be used to produce the resulting Kafka-Message after/while processing
// events from EventsPoll service.
func (e *EventsIO) ProduceResult() chan<- *model.KafkaResponse {
	return (chan<- *model.KafkaResponse)(e.resultInput)
}
