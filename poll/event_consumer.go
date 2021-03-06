package poll

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/pkg/errors"
)

// eventHandler handler for Consumer Messages
type eventHandler struct {
	eventRespChan chan<- model.Document
	readConfig    *ReadConfig
}

func (*eventHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Event-Consumer")
	return nil
}

func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Event-Consumer")
	close(e.eventRespChan)
	return errors.New("Event-Consumer unexpectedly closed")
}

func (e *eventHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("Event-Consumer Listening...")
	for {
		select {
		case <-session.Context().Done():
			return errors.New("Event-Consumer: session closed")
		case msg := <-claim.Messages():
			doc := model.Document{}
			err := json.Unmarshal(msg.Value, &doc)
			if err != nil {
				err = errors.Wrap(err, "Error: unable to Unmarshal Event")
				log.Println(err)
				session.MarkMessage(msg, "")
				continue
			}

			log.Printf("Received EventResponse with ID: %s", doc.UUID)

			e.eventRespChan <- doc
			session.MarkMessage(msg, "")
		}
	}
}
