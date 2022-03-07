package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/sudoblockio/icon-go-etl/models"
)

// ClaimCrud - type for claim table model
type ClaimCrud struct {
	db            *gorm.DB
	model         *models.Claim
	modelORM      *models.ClaimORM
	LoaderChannel chan *models.Claim
}

var claimCrud *ClaimCrud
var claimCrudOnce sync.Once

// GetClaimCrud - create and/or return the claims table model
func GetClaimCrud() *ClaimCrud {
	claimCrudOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		claimCrud = &ClaimCrud{
			db:            dbConn,
			model:         &models.Claim{},
			modelORM:      &models.ClaimORM{},
			LoaderChannel: make(chan *models.Claim, 1),
		}

		err := claimCrud.Migrate()
		if err != nil {
			zap.S().Fatal("ClaimCrud: Unable migrate postgres table: ", err.Error())
		}

		StartClaimLoader()
	})

	return claimCrud
}

// Migrate - migrate claims table
func (m *ClaimCrud) Migrate() error {
	// Only using ClaimRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

func (m *ClaimCrud) SelectOneClaim() (*models.Claim, error) {
	db := m.db

	claim := &models.Claim{}

	err := db.Transaction(func(tx *gorm.DB) error {

		// SELECT
		tx = tx.Raw("SELECT * FROM claims WHERE is_claimed = ? FOR UPDATE NOWAIT", false).Scan(claim)
		if tx.Error != nil {
			// Rollback
			return tx.Error
		}

		// UPDATE
		tx = tx.
			Where("job_hash = ?", claim.JobHash).
			Where("claim_index = ?", claim.ClaimIndex).
			Update("is_claimed", true)
		if tx.Error != nil {
			// Rollback
			return tx.Error
		}

		// Commit
		return nil
	})

	if err != nil {
		// Failed
		return nil, err
	}
	return claim, nil
}

func (m *ClaimCrud) SelectOneClaimHead() (*models.Claim, error) {
	db := m.db

	// NOTE Head claims should never set is_claim to true
	// NOTE Head claims should never set is_completed to true

	// Set table
	db = db.Model(&[]models.Claim{})

	// Job Hash - always "HEAD_CLAIM"
	db = db.Where("job_hash = ?", "HEAD_CLAIM")

	// Claim Index - always 0
	db = db.Where("claim_index = ?", 0)

	claim := &models.Claim{}
	db = db.First(claim)

	return claim, db.Error
}

func (m *ClaimCrud) UpdateOneComplete(claim *models.Claim) error {
	db := m.db

	// Set table
	db = db.Model(&[]models.Claim{})

	// Job Hash
	db = db.Where("job_hash = ?", claim.JobHash)

	// Claim Index
	db = db.Where("claim_index = ?", claim.ClaimIndex)

	db = db.Update("is_completed", true)

	return db.Error
}

func (m *ClaimCrud) UpsertOne(
	claim *models.Claim,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*claim),
		reflect.TypeOf(*claim),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "job_hash"}, {Name: "claim_index"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(claim)

	return db.Error
}

// StartClaimLoader starts loader
func StartClaimLoader() {
	go func() {

		for {
			// Read claim
			newClaim := <-GetClaimCrud().LoaderChannel

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetClaimCrud().UpsertOne(newClaim)
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Claim, JobHash=", newClaim.JobHash, " ClaimIndex=", newClaim.ClaimIndex, " - Error: ", err.Error())
			}
		}
	}()
}
