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
	log.Println("Initializing Kafka EventHandler")
	return nil
}

func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka EventHandler")
	return nil
}

func (e *eventHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("Listening for new Events...")
	for msg := range claim.Messages() {
		go func(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
			kr := model.KafkaResponse{}
			err := json.Unmarshal(msg.Value, &kr)
			if err != nil {
				err = errors.Wrap(err, "Error: unable to Unmarshal Event")
				log.Println(err)
				session.MarkMessage(msg, "")
				return
			}

			log.Printf("Received EventResponse with ID: %s", kr.UUID)

			e.eventRespChan <- kr
			session.MarkMessage(msg, "")
		}(session, msg)
	}
	return nil
}
