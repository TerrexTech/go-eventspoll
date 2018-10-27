package poll

import (
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("InitTest", func() {
	var ioConfig IOConfig

	BeforeEach(func() {
		kc := KafkaConfig{
			ESQueryResCons:  &kafka.ConsumerConfig{},
			EventCons:       &kafka.ConsumerConfig{},
			ESQueryReqProd:  &kafka.ProducerConfig{},
			SvcResponseProd: &kafka.ProducerConfig{},

			ESQueryReqTopic:  "testpeqt",
			SvcResponseTopic: "testrt",
		}
		mc := MongoConfig{
			AggCollection:      &mongo.Collection{},
			AggregateID:        2,
			Connection:         &mongo.ConnectionConfig{},
			MetaDatabaseName:   "test_db",
			MetaCollectionName: "test_coll",
		}
		ioConfig = IOConfig{
			ReadConfig:  ReadConfig{},
			KafkaConfig: kc,
			MongoConfig: mc,
		}
	})

	Describe("MongoConfig Validation", func() {
		It("should return error if AggregateID is not specified", func() {
			mc := ioConfig.MongoConfig

			mc.AggregateID = 0
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())

			mc.AggregateID = -2
			eventsIO, err = Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if AggCollection is not specified", func() {
			mc := ioConfig.MongoConfig

			mc.AggCollection = nil
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if Connection is not specified", func() {
			mc := ioConfig.MongoConfig

			mc.Connection = nil
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if MetaDatabaseName is not specified", func() {
			mc := ioConfig.MongoConfig

			mc.MetaDatabaseName = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if MetaCollectionName is not specified", func() {
			mc := ioConfig.MongoConfig

			mc.MetaDatabaseName = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})
	})

	Describe("KafkaConfig Validation", func() {
		It("should return error if ESQueryResCons is not specified", func() {
			kc := ioConfig.KafkaConfig
			kc.ESQueryResCons = nil
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if EventCons is not specified", func() {
			kc := ioConfig.KafkaConfig
			kc.EventCons = nil
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if ESQueryReqProd is not specified", func() {
			kc := ioConfig.KafkaConfig
			kc.ESQueryReqProd = nil
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if ESQueryReqTopic is not specified", func() {
			kfConfig := ioConfig.KafkaConfig

			kfConfig.ESQueryReqTopic = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})

		It("should return error if SvcResponseTopic is not specified", func() {
			kfConfig := ioConfig.KafkaConfig

			kfConfig.SvcResponseTopic = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())
		})
	})

	It("should return error on invalid ReadConfig", func() {
		// bool zero-value is false,
		// so we dont need to set every value here
		ioConfig.ReadConfig = ReadConfig{}

		eventsIO, err := Init(ioConfig)
		Expect(err).To(HaveOccurred())
		Expect(eventsIO).To(BeNil())
	})
})
