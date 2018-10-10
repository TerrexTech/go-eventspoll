package poll

import (
	ctx "context"
	"log"
	"time"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"github.com/pkg/errors"
)

// Difference collections can have different fail-counts, so we cache them in this map
var dbFailCache map[string]int16

// getMaxVersion returns the maximum event-hydration version for the Aggregate.
// This version is updated everytime new events are processed, except on "query" events.

// By default, if it fails to get the max aggregate-version from Mongo, it will return the
// version as 1, assuming that the aggregate-collection was simply empty. However, in case
// of Database failure, this could lead to repeatedly returning the value "1", causing
// service to fetch all events from beginning.
// So to prevent this, a temprary measure has been implemented to track the number of
// times the service failed to fetch the version from DB. After a certain limit, errors
// are thrown instead of assuming version to be 1. The "dbFailThreshold" is that limit.

// This is not an ideal method to deal
// with this, so a better method will be put in future.
func getMaxVersion(c *mongo.Collection, dbFailThreshold int16) (int64, error) {
	if dbFailCache == nil {
		dbFailCache = map[string]int16{}
	}

	findCtx, findCancel := ctx.WithTimeout(
		ctx.Background(),
		time.Duration(c.Connection.Timeout)*time.Millisecond,
	)
	defer findCancel()

	filter := map[string]interface{}{
		"version": map[string]int{
			"$gt": 0,
		},
	}
	opt := findopt.Sort(
		map[string]interface{}{
			"version": -1,
		},
	)
	result := map[string]interface{}{}
	err := c.Collection().FindOne(findCtx, filter, opt).Decode(result)
	if err != nil {
		// Return error if getting version has failed enough times
		dbFailCache[c.Name] = dbFailCache[c.Name] + 1
		if dbFailCache[c.Name] > dbFailThreshold {
			return -1, err
		}

		err = errors.Wrap(err, "Error fetching max version")
		log.Println(err)
		log.Println("Version will be assumed to be 1 (new Aggregate)")
		return 1, nil
	}
	// Reset fail-counter on successful connection
	dbFailCache[c.Name] = 0

	version, ok := result["version"].(int64)
	if !ok {
		err = errors.New("GetMaxVersion Error: Unable to get field 'version'")
		return -1, err
	}
	return version, nil
}
