package services

import (
	"github.com/mahendraintelops/mm-project1/device-service/pkg/rest/server/daos"
	"github.com/mahendraintelops/mm-project1/device-service/pkg/rest/server/models"
)

type DeviceService struct {
	deviceDao *daos.DeviceDao
}

func NewDeviceService() (*DeviceService, error) {
	deviceDao, err := daos.NewDeviceDao()
	if err != nil {
		return nil, err
	}
	return &DeviceService{
		deviceDao: deviceDao,
	}, nil
}

func (deviceService *DeviceService) CreateDevice(device *models.Device) (*models.Device, error) {
	return deviceService.deviceDao.CreateDevice(device)
}

func (deviceService *DeviceService) ListDevices() ([]*models.Device, error) {
	return deviceService.deviceDao.ListDevices()
}

func (deviceService *DeviceService) GetDevice(id int64) (*models.Device, error) {
	return deviceService.deviceDao.GetDevice(id)
}
