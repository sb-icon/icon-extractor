package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-go-etl/models"
)

// BatchCrud - type for batch table model
type BatchCrud struct {
	db            *gorm.DB
	model         *models.Batch
	modelORM      *models.BatchORM
	LoaderChannel chan *models.Batch
}

var batchCrud *BatchCrud
var batchCrudOnce sync.Once

// GetBatchCrud - create and/or return the batchs table model
func GetBatchCrud() *BatchCrud {
	batchCrudOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		batchCrud = &BatchCrud{
			db:            dbConn,
			model:         &models.Batch{},
			modelORM:      &models.BatchORM{},
			LoaderChannel: make(chan *models.Batch, 1),
		}

		err := batchCrud.Migrate()
		if err != nil {
			zap.S().Fatal("BatchCrud: Unable migrate postgres table: ", err.Error())
		}

		StartBatchLoader()
	})

	return batchCrud
}

// Migrate - migrate batchs table
func (m *BatchCrud) Migrate() error {
	// Only using BatchRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

func (m *BatchCrud) UpsertOne(
	batch *models.Batch,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*batch),
		reflect.TypeOf(*batch),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "job_hash"}, {Name: "batch_index"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(batch)

	return db.Error
}

// StartBatchLoader starts loader
func StartBatchLoader() {
	go func() {

		for {
			// Read batch
			newBatch := <-GetBatchCrud().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err = GetBatchCrud().UpsertOne(newBatch)
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Batch, JobHash=", newBatch.JobHash, " BatchIndex=", newBatch.BatchIndex, " - Error: ", err.Error())
			}
		}
	}()
}
