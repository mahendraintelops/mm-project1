package daos

import (
	"database/sql"
	"errors"
	"github.com/go-sql-driver/mysql"
	"github.com/mahendraintelops/mm-project1/device-service/pkg/rest/server/daos/clients/sqls"
	"github.com/mahendraintelops/mm-project1/device-service/pkg/rest/server/models"
	log "github.com/sirupsen/logrus"
)

type DeviceDao struct {
	sqlClient *sqls.MySQLClient
}

func migrateDevices(r *sqls.MySQLClient) error {
	query := `
	CREATE TABLE IF NOT EXISTS devices(
		ID int NOT NULL AUTO_INCREMENT,
        
		Name VARCHAR(100) NOT NULL,
	    PRIMARY KEY (ID)
	);
	`
	_, err := r.DB.Exec(query)
	return err
}

func NewDeviceDao() (*DeviceDao, error) {
	sqlClient, err := sqls.InitMySQLDB()
	if err != nil {
		return nil, err
	}
	err = migrateDevices(sqlClient)
	if err != nil {
		return nil, err
	}
	return &DeviceDao{
		sqlClient,
	}, nil
}

func (deviceDao *DeviceDao) CreateDevice(m *models.Device) (*models.Device, error) {
	insertQuery := "INSERT INTO devices(Name) values(?)"
	res, err := deviceDao.sqlClient.DB.Exec(insertQuery, m.Name)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) {
			if mysqlErr.Number == 1062 {
				return nil, sqls.ErrDuplicate
			}
		}
		return nil, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	m.Id = id
	log.Debugf("device created")
	return m, nil
}

func (deviceDao *DeviceDao) ListDevices() ([]*models.Device, error) {
	selectQuery := "SELECT * FROM devices"
	rows, err := deviceDao.sqlClient.DB.Query(selectQuery)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	var devices []*models.Device
	for rows.Next() {
		m := models.Device{}
		if err = rows.Scan(&m.Id, &m.Name); err != nil {
			return nil, err
		}
		devices = append(devices, &m)
	}
	if devices == nil {
		devices = []*models.Device{}
	}
	log.Debugf("device listed")
	return devices, nil
}

func (deviceDao *DeviceDao) GetDevice(id int64) (*models.Device, error) {
	selectQuery := "SELECT * FROM devices WHERE Id = ?"
	row := deviceDao.sqlClient.DB.QueryRow(selectQuery, id)

	m := models.Device{}
	if err := row.Scan(&m.Id, &m.Name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sqls.ErrNotExists
		}
		return nil, err
	}
	log.Debugf("device retrieved")
	return &m, nil
}
