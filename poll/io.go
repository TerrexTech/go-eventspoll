package poll

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/TerrexTech/go-eventstore-models/model"
)

// EventsIO allows interacting with the EventsPoll service.
// This is the medium through which the new events are distributed.
type EventsIO struct {
	closeChan   chan<- struct{}
	errGroup    *errgroup.Group
	errGroupCtx context.Context

	delete      chan *EventResponse
	insert      chan *EventResponse
	query       chan *EventResponse
	update      chan *EventResponse
	resultInput chan<- *model.KafkaResponse
}

func newEventsIO(
	errGroupCtx context.Context,
	errGroup *errgroup.Group,
	closeChan chan<- struct{},
	resultInput chan<- *model.KafkaResponse,
) *EventsIO {
	return &EventsIO{
		closeChan:   closeChan,
		errGroup:    errGroup,
		errGroupCtx: errGroupCtx,

		delete:      make(chan *EventResponse, 256),
		insert:      make(chan *EventResponse, 256),
		query:       make(chan *EventResponse, 256),
		update:      make(chan *EventResponse, 256),
		resultInput: resultInput,
	}
}

// RoutinesGroup returns the errgroup used for EventsPoll-routines.
func (e *EventsIO) RoutinesGroup() *errgroup.Group {
	return e.errGroup
}

// RoutinesCtx returns the errgroup-context used for EventsPoll-routines.
func (e *EventsIO) RoutinesCtx() context.Context {
	return e.errGroupCtx
}

// Wait is a wrapper for errgroup.Wait, and will wait for all EventsPoll and RoutinesGroup
// routines to exit, and propagate the error from errgroup.Wait.
// This returns new channel everytime called, so it can be used multiple times in
// different places.
// Note: The resulting channel will get data only once, and is then closed.
func (e *EventsIO) Wait() <-chan error {
	waitChan := make(chan error)
	go func() {
		err := e.errGroup.Wait()
		waitChan <- err
		close(waitChan)
	}()
	return (<-chan error)(waitChan)
}

// Close closes any open routines associated with EventsPoll service, such as
// Kafka Producers and Consumers. Use this when the service is no longer required.
func (e *EventsIO) Close() {
	e.closeChan <- struct{}{}
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
