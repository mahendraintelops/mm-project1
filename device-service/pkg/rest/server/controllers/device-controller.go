package controllers

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/mahendraintelops/mm-project1/device-service/pkg/rest/server/daos/clients/sqls"
	"github.com/mahendraintelops/mm-project1/device-service/pkg/rest/server/models"
	"github.com/mahendraintelops/mm-project1/device-service/pkg/rest/server/services"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"os"
	"strconv"
)

type DeviceController struct {
	deviceService *services.DeviceService
}

func NewDeviceController() (*DeviceController, error) {
	deviceService, err := services.NewDeviceService()
	if err != nil {
		return nil, err
	}
	return &DeviceController{
		deviceService: deviceService,
	}, nil
}

func (deviceController *DeviceController) CreateDevice(context *gin.Context) {
	// validate input
	var input models.Device
	if err := context.ShouldBindJSON(&input); err != nil {
		log.Error(err)
		context.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		return
	}

	// trigger device creation
	deviceCreated, err := deviceController.deviceService.CreateDevice(&input)
	if err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	context.JSON(http.StatusCreated, deviceCreated)
}

func (deviceController *DeviceController) ListDevices(context *gin.Context) {
	// trigger all devices fetching
	devices, err := deviceController.deviceService.ListDevices()
	if err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	context.JSON(http.StatusOK, devices)
}

func (deviceController *DeviceController) FetchDevice(context *gin.Context) {
	id, err := strconv.ParseInt(context.Param("id"), 10, 64)
	if err != nil {
		log.Error(err)
		context.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// trigger device fetching
	device, err := deviceController.deviceService.GetDevice(id)
	if err != nil {
		log.Error(err)
		if errors.Is(err, sqls.ErrNotExists) {
			context.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	serviceName := os.Getenv("SERVICE_NAME")
	collectorURL := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if len(serviceName) > 0 && len(collectorURL) > 0 {
		// get the current span by the request context
		currentSpan := trace.SpanFromContext(context.Request.Context())
		currentSpan.SetAttributes(attribute.String("device.id", strconv.FormatInt(device.Id, 10)))
	}

	context.JSON(http.StatusOK, device)
}
