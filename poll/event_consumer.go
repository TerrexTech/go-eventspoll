package poll

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/pkg/errors"
)

// eventHandler handler for Consumer Messages
type eventHandler struct {
	eventRespChan chan<- model.KafkaResponse
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
			kr := model.KafkaResponse{}
			err := json.Unmarshal(msg.Value, &kr)
			if err != nil {
				err = errors.Wrap(err, "Error: unable to Unmarshal Event")
				log.Println(err)
				session.MarkMessage(msg, "")
				continue
			}

			log.Printf("Received EventResponse with ID: %s", kr.UUID)

			e.eventRespChan <- kr
			session.MarkMessage(msg, "")
		}
	}
}
