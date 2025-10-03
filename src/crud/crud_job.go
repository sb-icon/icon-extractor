package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/sb-icon/icon-extractor/models"
)

// JobCrud - type for job table model
type JobCrud struct {
	db            *gorm.DB
	model         *models.Job
	modelORM      *models.JobORM
	LoaderChannel chan *models.Job
}

var jobCrud *JobCrud
var jobCrudOnce sync.Once

// GetJobCrud - create and/or return the jobs table model
func GetJobCrud() *JobCrud {
	jobCrudOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		jobCrud = &JobCrud{
			db:            dbConn,
			model:         &models.Job{},
			modelORM:      &models.JobORM{},
			LoaderChannel: make(chan *models.Job, 1),
		}

		err := jobCrud.Migrate()
		if err != nil {
			zap.S().Fatal("JobCrud: Unable migrate postgres table: ", err.Error())
		}

		StartJobLoader()
	})

	return jobCrud
}

// Migrate - migrate jobs table
func (m *JobCrud) Migrate() error {
	// Only using JobRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// SelectOne - select from blocks table
func (m *JobCrud) SelectOne(
	hash string,
) (*models.Job, error) {
	db := m.db

	db = db.Where("hash = ?", hash)

	job := &models.Job{}
	db = db.First(job)

	return job, db.Error
}

func (m *JobCrud) UpsertOne(
	job *models.Job,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*job),
		reflect.TypeOf(*job),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(job)

	return db.Error
}

// StartJobLoader starts loader
func StartJobLoader() {
	go func() {

		for {
			// Read job
			newJob := <-GetJobCrud().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetJobCrud().UpsertOne(newJob)
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Job, Hash=", newJob.Hash, " - Error: ", err.Error())
			}
		}
	}()
}
