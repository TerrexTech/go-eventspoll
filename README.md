EventsPoll
---

This library distributes new events to specific channels as per their `EventAction` type, in a fan-out approach.

How it works:

* Listen for new events on specified topic.
* Filter events and only process ones for the specified Aggregate.
* Get the Max Aggregate-Version from Database.
* Send request to [EventStoreQuery][0] with the Max Aggregate-Version.
* Get events from EventStoreQuery.
* Fan-Out events to their respective channels based on `Event.EventAction`.

---

### How to use

* Check [**examples**][1] for how to use this library.
* Check [**tests-file**][2] for additional examples.

---

* [Go Docs][3]

---

  [0]: https://github.com/TerrexTech/go-eventstore-query
  [1]: https://github.com/TerrexTech/go-eventspoll/blob/master/examples/example.go
  [2]: https://github.com/TerrexTech/go-eventspoll/blob/master/poll/poll_suite_test.go
  [3]: https://godoc.org/github.com/TerrexTech/go-eventspoll/poll
