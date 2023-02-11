package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	vallox "github.com/jokujossai/vallox-rs485"

	"github.com/kelseyhightower/envconfig"

	mqttClient "github.com/eclipse/paho.mqtt.golang"
)

type cacheEntry struct {
	time  time.Time
	value vallox.Event
}

const (
	topicIO7Raw       = "vallox/io7/raw"
	topicIO7Reheating = "vallox/io7/reheating"

	topicIO8Raw             = "vallox/io8/raw"
	topicIO8SummerMode      = "vallox/io8/summerMode"
	topicIO8ErrorRelay      = "vallox/io8/errorRelay"
	topicIO8MotorIn         = "vallox/io8/motorIn"
	topicIO8Preheating      = "vallox/io8/preheating"
	topicIO8MotorOut        = "vallox/io8/motorOut"
	topicIO8FireplaceSwitch = "vallox/io8/fireplaceSwitch"

	topicFanCurrentSpeed = "vallox/fan/currentSpeed"
	topicFanSpeedSet     = "vallox/fan/set"
	topicFanMaxSpeed     = "vallox/fan/max"
	topicFanDefaultSpeed = "vallox/fan/default"

	topicRHMax   = "vallox/rh/max"
	topicRH1     = "vallox/rh/1"
	topicRH2     = "vallox/rh/2"
	topicRHBasic = "vallox/rh/basic"

	topicCO2Current   = "vallox/co2/current"
	topicCO2Max       = "vallox/co2/max"
	topicCO2SensorRaw = "vallox/co2/installed/raw"
	topicCO2Sensor1   = "vallox/co2/installed/sensor1"
	topicCO2Sensor2   = "vallox/co2/installed/sensor2"
	topicCO2Sensor3   = "vallox/co2/installed/sensor3"
	topicCO2Sensor4   = "vallox/co2/installed/sensor4"
	topicCO2Sensor5   = "vallox/co2/installed/sensor5"

	topicMessage = "vallox/message/value"

	topicTempOutdoor    = "vallox/temp/outdoor"
	topicTempExhaustOut = "vallox/temp/exhaustOut"
	topicTempExhaustIn  = "vallox/temp/exhaustIn"
	topicTempSupply     = "vallox/temp/supply"

	topicFaultRaw               = "vallox/fault/raw"
	topicFaultSupplySensor      = "vallox/fault/supplySensor"
	topicFaultCO2Alarm          = "vallox/fault/CO2Alarm"
	topicFaultOutdoorSensor     = "vallox/fault/outdoorSensor"
	topicFaultExhaustInSensor   = "vallox/fault/exhaustInSensor"
	topicFaultWaterCoilFreezing = "vallox/fault/waterCoilFreezing"
	topicFaultExhaustOutSensor  = "vallox/fault/exhaustOutSensor"

	topicPostHeatingOnTime     = "vallox/postHeating/onTime"
	topicPostHeatingOffTime    = "vallox/postHeating/offTime"
	topicPostHeatingTargetTemp = "vallox/postHeating/targetTemp"

	topicFlags2Raw                 = "vallox/flags2/raw"
	topicFlags2CO2HigherSpeedReq   = "vallox/flags2/CO2HigherSpeedReq"
	topicFlags2CO2LowerSpeedReq    = "vallox/flags2/CO2LoweSpeedReq"
	topicFlags2RHLowerSpeedReq     = "vallox/flags2/RHLowerSpeedReq"
	topicFlags2SwitchLowerSpeedReq = "vallox/flags2/switchLowerSpeedReq"
	topicFlags2CO2Alarm            = "vallox/flags2/CO2Alarm"
	topicFlags2CellFreezeAlarm     = "vallox/flags2/cellFreezeAlarm"

	topicFlags4Raw               = "vallox/flags4/raw"
	topicFlags4WaterCoilFreezing = "vallox/flags4/waterCoilFreezing"
	topicFlags4Master            = "vallox/flags4/master"

	topicFlags5Raw              = "vallox/flags5/raw"
	topicFlags5PreheatingStatus = "vallox/flags5/preheatingStatus"

	topicFlags6Raw              = "vallox/flags6/raw"
	topicFlags6RemoteControl    = "vallox/flags6/remoteControl"
	topicFlags6FireplaceSwitch  = "vallox/flags6/fireplaceSwitch"
	topicFlags6FireplaceFuncion = "vallox/flags6/fireplaceFunction"

	topicFireplaceSwitchCounter = "vallox/fireplace/counter"

	topicStatusRaw            = "vallox/status/raw"
	topicStatusPower          = "vallox/status/power"
	topicStatusCO2            = "vallox/status/CO2"
	topicStatusRH             = "vallox/status/RH"
	topicStatusPostHeatingKey = "vallox/status/postHeatingKey"
	topicStatusFilterGuard    = "vallox/status/filterQuard"
	topicStatusPostHeatingLed = "vallox/status/postHeatingLed"
	topicStatusFault          = "vallox/status/fault"
	topicStatusService        = "vallox/status/service"

	topicPostHeatingSetpoint = "vallox/postHeating/setPointTemp"
	topicPreHeatingSwitching = "vallox/preHeating/switchingTemp"
	topicSupplyFanStop       = "vallox/supplyFan/stopTemp"
	topicBypassOperating     = "vallox/bypass/operatingTemp"

	topicServiceReminderInterval = "vallox/serviceReminder/interval"
	topicServiceReminderCounter  = "vallox/serviceReminder/counter"

	topicProgramRaw               = "vallox/program/raw"
	topicProgramAutomaticHumidity = "vallox/program/automaticHymidity"
	topicProgramFireplaceSwitch   = "vallox/program/fireplaceSwitch"
	topicProgramWater             = "vallox/program/water"
	topicProgramCascadeControl    = "vallox/program/cascadeControl"

	topicSupplyFanControlSetpoint  = "vallox/supplyFan/controlSetpoint"
	topicExhaustFanControlSetpoint = "vallox/exhaustFan/controlSetpoint"
	topicCellAntifreezeHysteresis  = "vallox/cellAntiFreeze/hysteresis"

	topicCO2ControlSetpointUpper = "vallox/co2/controlSetpoint/upper"
	topicCO2ControlSetpointLower = "vallox/co2/controlSetpoint/lower"

	topicProgram2Raw      = "vallox/program2/raw"
	topicProgram2MaxSpeed = "vallox/program2/maxSpeed"
)

var topicMap = map[byte]string{
	vallox.RegisterIO07:                 topicIO7Raw,
	vallox.RegisterIO08:                 topicIO8Raw,
	vallox.RegisterCurrentFanSpeed:      topicFanCurrentSpeed,
	vallox.RegisterMaxRH:                topicRHMax,
	vallox.RegisterCurrentCO2:           topicCO2Current,
	vallox.RegisterMaximumCO2:           topicCO2Max,
	vallox.RegisterCO2Status:            topicCO2SensorRaw,
	vallox.RegisterMessage:              topicMessage,
	vallox.RegisterRH1:                  topicRH1,
	vallox.RegisterRH2:                  topicRH2,
	vallox.RegisterOutdoorTemp:          topicTempOutdoor,
	vallox.RegisterExhaustOutTemp:       topicTempExhaustOut,
	vallox.RegisterExhaustInTemp:        topicTempExhaustIn,
	vallox.RegisterSupplyTemp:           topicTempSupply,
	vallox.RegisterFaultCode:            topicFaultRaw,
	vallox.RegisterPostHeatingOnTime:    topicPostHeatingOnTime,
	vallox.RegisterPostHeatingOffTime:   topicPostHeatingOffTime,
	vallox.RegisterFlags02:              topicFlags2Raw,
	vallox.RegisterFlags04:              topicFlags4Raw,
	vallox.RegisterFlags05:              topicFlags5Raw,
	vallox.RegisterFlags06:              topicFlags6Raw,
	vallox.RegisterFireplaceCounter:     topicFireplaceSwitchCounter,
	vallox.RegisterStatus:               topicStatusRaw,
	vallox.RegisterPostHeatingSetpoint:  topicPostHeatingSetpoint,
	vallox.RegisterMaxFanSpeed:          topicFanMaxSpeed,
	vallox.RegisterServiceInterval:      topicServiceReminderInterval,
	vallox.RegisterPreheatingTemp:       topicPreHeatingSwitching,
	vallox.RegisterSupplyFanStopTemp:    topicSupplyFanStop,
	vallox.RegisterDefaultFanSpeed:      topicFanDefaultSpeed,
	vallox.RegisterProgram:              topicProgramRaw,
	vallox.RegisterServiceCounter:       topicServiceReminderCounter,
	vallox.RegisterBasicHumidity:        topicRHBasic,
	vallox.RegisterBypassTemp:           topicBypassOperating,
	vallox.RegisterSupplyFanSetpoint:    topicSupplyFanControlSetpoint,
	vallox.RegisterExhaustFanSetpoint:   topicExhaustFanControlSetpoint,
	vallox.RegisterAntiFreezeHysteresis: topicCellAntifreezeHysteresis,
	vallox.RegisterCO2SetpointUpper:     topicCO2ControlSetpointUpper,
	vallox.RegisterCO2SetpointLower:     topicCO2ControlSetpointLower,
	vallox.RegisterProgram2:             topicProgram2Raw,
}

var topicFlagMap = map[byte]map[byte]string{
	vallox.RegisterIO07: map[byte]string{
		vallox.IO07FlagReheating: topicIO7Reheating,
	},
	vallox.RegisterIO08: map[byte]string{
		vallox.IO08FlagSummerMode:      topicIO8SummerMode,
		vallox.IO08FlagErrorRelay:      topicIO8ErrorRelay,
		vallox.IO08FlagMotorIn:         topicIO8MotorIn,
		vallox.IO08FlagPreheating:      topicIO8Preheating,
		vallox.IO08FlagMotorOut:        topicIO8MotorOut,
		vallox.IO08FlagFireplaceSwitch: topicIO8FireplaceSwitch,
	},
	vallox.RegisterCO2Status: map[byte]string{
		vallox.CO2Sensor1: topicCO2Sensor1,
		vallox.CO2Sensor2: topicCO2Sensor2,
		vallox.CO2Sensor3: topicCO2Sensor3,
		vallox.CO2Sensor4: topicCO2Sensor4,
	},
	vallox.RegisterFaultCode: map[byte]string{
		vallox.FaultSupplyAirSensorFault:     topicFaultSupplySensor,
		vallox.FaultCarbonDioxideAlarm:       topicFaultCO2Alarm,
		vallox.FaultOutdoorSensorFault:       topicFaultOutdoorSensor,
		vallox.FaultExhaustAirInSensorFault:  topicFaultExhaustInSensor,
		vallox.FaultWaterCoilFreezing:        topicFaultWaterCoilFreezing,
		vallox.FaultExhaustAirOutSensorFault: topicFaultExhaustOutSensor,
	},
	vallox.RegisterFlags02: map[byte]string{
		vallox.Flags2CO2HigherSpeedReq:   topicFlags2CO2HigherSpeedReq,
		vallox.Flags2CO2LowerSpeedReq:    topicFlags2CO2LowerSpeedReq,
		vallox.Flags2RHLowerSpeedReq:     topicFlags2RHLowerSpeedReq,
		vallox.Flags2SwitchLowerSpeedReq: topicFlags2SwitchLowerSpeedReq,
		vallox.Flags2CO2Alarm:            topicFlags2CO2Alarm,
		vallox.Flags2CellFreezeAlarm:     topicFlags2CellFreezeAlarm,
	},
	vallox.RegisterFlags04: map[byte]string{
		vallox.Flags4WaterCoilFreezing: topicFlags4WaterCoilFreezing,
		vallox.Flags4Master:            topicFlags4Master,
	},
	vallox.RegisterFlags05: map[byte]string{
		vallox.Flags5PreheatingStatus: topicFlags5PreheatingStatus,
	},
	vallox.RegisterFlags06: map[byte]string{
		vallox.Flags6RemoteControl:           topicFlags6RemoteControl,
		vallox.Flags6ActivateFireplaceSwitch: topicFlags6FireplaceSwitch,
		vallox.Flags6FireplaceFunction:       topicFlags6FireplaceFuncion,
	},
	vallox.RegisterStatus: map[byte]string{
		vallox.StatusFlagPower:       topicStatusPower,
		vallox.StatusFlagCO2:         topicStatusCO2,
		vallox.StatusFlagRH:          topicStatusRH,
		vallox.StatusFlagHeatingMode: topicStatusPostHeatingKey,
		vallox.StatusFlagFilter:      topicStatusFilterGuard,
		vallox.StatusFlagHeating:     topicStatusPostHeatingLed,
		vallox.StatusFlagFault:       topicStatusFault,
		vallox.StatusFlagService:     topicStatusService,
	},
	vallox.RegisterProgram: map[byte]string{
		vallox.ProgramFlagAutomaticHumidity: topicProgramAutomaticHumidity,
		vallox.ProgramFlagBoostSwitch:       topicProgramFireplaceSwitch,
		vallox.ProgramFlagWater:             topicProgramWater,
		vallox.ProgramFlagCascadeControl:    topicProgramCascadeControl,
	},
	vallox.RegisterProgram2: map[byte]string{
		vallox.Program2FlagMaximumSpeedLimit: topicProgram2MaxSpeed,
	},
}

// TODO: Configurable
var device = map[string]interface{}{
	"identifiers": []string{
		"vallox",
	},
	"manufacturer": "Vallox",
	"name":         "Vallox Digit SE",
	"model":        "Digit SE",
}

var discovery = map[string][]map[string]interface{}{
	"binary_sensor": {
		map[string]interface{}{
			"unique_id":    "vallox_io7_reheating",
			"name":         "Jälkilämmitys",
			"device":       device,
			"device_class": "heat",
			"state_topic":  topicIO7Reheating,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_io8_summer_mode",
			"name":        "Peltimoottorin asento (kesä)",
			"device":      device,
			"state_topic": topicIO8SummerMode,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_io8_error_relay",
			"name":        "Vikatietorele",
			"device":      device,
			"state_topic": topicIO8ErrorRelay,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_io8_flag_motor_in",
			"name":        "Tulopuhallin",
			"device":      device,
			"state_topic": topicIO8MotorIn,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_io8_preheating",
			"name":         "Etulämmitys",
			"device":       device,
			"device_class": "heat",
			"state_topic":  topicIO8Preheating,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_io8_motor_out",
			"name":        "Poistopuhallin",
			"device":      device,
			"state_topic": topicIO8MotorOut,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_io8_fireplace_switch",
			"name":        "Takka/tehostuskytkin",
			"device":      device,
			"state_topic": topicIO8FireplaceSwitch,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_status_power",
			"name":         "Virtanäppäin",
			"device":       device,
			"device_class": "plug",
			"state_topic":  topicStatusPower,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_co2_status_1",
			"name":        "CO2 anturi 1",
			"device":      device,
			"state_topic": topicCO2Sensor1,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_co2_status_2",
			"name":        "CO2 anturi 2",
			"device":      device,
			"state_topic": topicCO2Sensor2,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_co2_status_3",
			"name":        "CO2 anturi 3",
			"device":      device,
			"state_topic": topicCO2Sensor3,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_co2_status_4",
			"name":        "CO2 anturi 4",
			"device":      device,
			"state_topic": topicCO2Sensor4,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_fault_supply_sensor",
			"name":         "Tuloilma-anturivika",
			"device":       device,
			"device_class": "problem",
			"state_topic":  topicFaultSupplySensor,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_fault_co2_alarm",
			"name":         "Hiilidioksidihälytys",
			"device":       device,
			"device_class": "problem",
			"state_topic":  topicFaultCO2Alarm,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_fault_outdoor_sensor",
			"name":         "Ulkoilma-anturivika",
			"device":       device,
			"device_class": "problem",
			"state_topic":  topicFaultOutdoorSensor,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_fault_exhaust_in",
			"name":         "Poistoilma-anturivika",
			"device":       device,
			"device_class": "problem",
			"state_topic":  topicFaultExhaustInSensor,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_fault_water_coil_freezing",
			"name":         "Vesipatterin jäätymisvaara",
			"device":       device,
			"device_class": "problem",
			"state_topic":  topicFaultWaterCoilFreezing,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_fault_exhaust_out",
			"name":         "Jäteilma-anturivika",
			"device":       device,
			"device_class": "problem",
			"state_topic":  topicFaultExhaustOutSensor,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_flags2_co2_higher_speed_req",
			"name":        "CO2 suurempi nopeus -pyyntö",
			"device":      device,
			"state_topic": topicFlags2CO2HigherSpeedReq,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_flags2_co2_lower_speed_req",
			"name":        "CO2 pienempi nopeus -pyyntö",
			"device":      device,
			"state_topic": topicFlags2CO2LowerSpeedReq,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_flags2_rh_lower_speed_req",
			"name":        "%RH pienempi nopeus -pyyntö",
			"device":      device,
			"state_topic": topicFlags2RHLowerSpeedReq,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_flags2_switch_lower_speed_req",
			"name":        "Kytkin pien. nop. -pyyntö",
			"device":      device,
			"state_topic": topicFlags2SwitchLowerSpeedReq,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_flags2_co2_alarm",
			"name":         "CO2 -hälytys",
			"device":       device,
			"device_class": "problem",
			"state_topic":  topicFlags2CO2Alarm,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_flags2_cell_freeze_alarm",
			"name":         "Kennon jäätymishälytys",
			"device":       device,
			"device_class": "problem",
			"state_topic":  topicFlags2CellFreezeAlarm,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":    "vallox_flags4_water_coil_freezing_alert",
			"name":         "Vesipatterin jäätymisvaara",
			"device":       device,
			"device_class": "problem",
			"state_topic":  topicFlags4WaterCoilFreezing,
			"payload_on":   "true",
			"payload_off":  "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_flags4_master",
			"name":        "slave(false)/master(true) valinta",
			"device":      device,
			"state_topic": topicFlags4Master,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_flags5_preheating_status",
			"name":        "Etulämmityksen tilalippu",
			"device":      device,
			"state_topic": topicFlags5PreheatingStatus,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_flags6_remote_control",
			"name":        "Kaukovalvontaohjaus",
			"device":      device,
			"state_topic": topicFlags6RemoteControl,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_flags6_fireplace_switch_activation",
			"name":        "Takkakykimen aktivointi",
			"device":      device,
			"state_topic": topicFlags6FireplaceSwitch,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_flags6_fireplace_function_state",
			"name":        "Takka/tehostustoiminto",
			"device":      device,
			"state_topic": topicFlags6FireplaceFuncion,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_status_power",
			"name":        "Virtanäppäin",
			"device":      device,
			"state_topic": topicStatusPower,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_status_co2_key",
			"name":        "CO2 -näppäin",
			"device":      device,
			"state_topic": topicStatusCO2,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_status_rh_key",
			"name":        "%RH -näppäin",
			"device":      device,
			"state_topic": topicStatusRH,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_status_post_heating_key",
			"name":        "Jälkilämmityksen näppäin",
			"device":      device,
			"state_topic": topicStatusPostHeatingKey,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_status_filter_guard_led",
			"name":        "Suodatinvahdin merkkivalo",
			"device":      device,
			"state_topic": topicStatusFilterGuard,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_status_post_heating_led",
			"name":        "Jälkilämmityksen merkkivalo",
			"device":      device,
			"state_topic": topicStatusPostHeatingLed,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_status_fault_led",
			"name":        "Vian merkkivalo",
			"device":      device,
			"state_topic": topicStatusFault,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_status_service_reminder",
			"name":        "Huoltomuistutin",
			"device":      device,
			"state_topic": topicStatusService,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_program_automatic_humidity",
			"name":        "Kosteustason automaattihaku",
			"device":      device,
			"state_topic": topicProgramAutomaticHumidity,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_program_fireplace_switch",
			"name":        "tehostus(on)/takkakytkimen(off) tila",
			"device":      device,
			"state_topic": topicProgramFireplaceSwitch,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_program_water",
			"name":        "Vesi(on)/sähköpatterimalli(off)",
			"device":      device,
			"state_topic": topicProgramWater,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_program_cascade_control",
			"name":        "Kaskadisäätö",
			"device":      device,
			"state_topic": topicProgramCascadeControl,
			"payload_on":  "true",
			"payload_off": "false",
		},
		map[string]interface{}{
			"unique_id":   "vallox_program2_max_speed",
			"name":        "Maksiminopeuden rajoitus",
			"device":      device,
			"state_topic": topicProgram2MaxSpeed,
			"payload_on":  "true",
			"payload_off": "false",
		},
	},
	"sensor": {
		map[string]interface{}{
			"unique_id":    "vallox_rh_max",
			"name":         "Nykyinen maksimi ilmankosteus",
			"device":       device,
			"device_class": "humidity",
			"state_topic":  topicRHMax,
		},
		// TODO: CO2 upper | lower
		map[string]interface{}{
			"unique_id":   "vallox_message",
			"name":        "Milliampeeri-/jänniteviesti",
			"device":      device,
			"state_topic": topicMessage,
		},
		map[string]interface{}{
			"unique_id":    "vallox_rh_1",
			"name":         "%RH #1",
			"device":       device,
			"device_class": "humidity",
			"state_topic":  topicRH1,
		},
		map[string]interface{}{
			"unique_id":    "vallox_rh_2",
			"name":         "%RH #2",
			"device":       device,
			"device_class": "humidity",
			"state_topic":  topicRH2,
		},
		map[string]interface{}{
			"unique_id":    "vallox_temp_outdoor",
			"name":         "Ulkolämpötila",
			"device":       device,
			"device_class": "temperature",
			"state_topic":  topicTempOutdoor,
		},
		map[string]interface{}{
			"unique_id":    "vallox_temp_exhaust_out",
			"name":         "Jäteilman lämpötila",
			"device":       device,
			"device_class": "temperature",
			"state_topic":  topicTempExhaustOut,
		},
		map[string]interface{}{
			"unique_id":    "vallox_temp_exhaust_in",
			"name":         "Poistoilman lämpötila",
			"device":       device,
			"device_class": "temperature",
			"state_topic":  topicTempExhaustIn,
		},
		map[string]interface{}{
			"unique_id":    "vallox_temp_supply",
			"name":         "Tuloilman lämpötila",
			"device":       device,
			"device_class": "temperature",
			"state_topic":  topicTempSupply,
		},
		map[string]interface{}{
			"unique_id":   "vallox_post_heating_on_time",
			"name":        "Jälilämmityksen ON-laskuri",
			"device":      device,
			"state_topic": topicPostHeatingOnTime,
		},
		map[string]interface{}{
			"unique_id":   "vallox_post_heating_off_time",
			"name":        "Jälkilämmityksen OFF-aika",
			"device":      device,
			"state_topic": topicPostHeatingOffTime,
		},
		map[string]interface{}{
			"unique_id":   "vallox_post_heating_target_temp",
			"name":        "Jäkilämmityksen kohdearvo",
			"device":      device,
			"state_topic": topicPostHeatingTargetTemp,
		},
		map[string]interface{}{
			"unique_id":   "vallox_fireplace_switch_counter",
			"name":        "Takka/tehostuskytkimen laskuri",
			"device":      device,
			"state_topic": topicFireplaceSwitchCounter,
		},
		map[string]interface{}{
			"unique_id":   "vallox_post_heating_set_point",
			"name":        "Jälkilämmityksen asetusarvo",
			"device":      device,
			"state_topic": topicPostHeatingSetpoint,
		},
		map[string]interface{}{
			"unique_id":   "vallox_max_fan_speed",
			"name":        "Maksimipuhallinnopeus",
			"device":      device,
			"icon":        "mdi:fan",
			"state_topic": topicFanMaxSpeed,
		},
		map[string]interface{}{
			"unique_id":    "vallox_service_reminder_interval",
			"name":         "Huoltomuistuttimen aikaväli",
			"device":       device,
			"device_class": "duration",
			"state_topic":  topicServiceReminderInterval,
		},
		map[string]interface{}{
			"unique_id":    "vallox_pre_heating_switching",
			"name":         "Etulämmityksen kytkentälämpötila",
			"device":       device,
			"device_class": "temperature",
			"state_topic":  topicPreHeatingSwitching,
		},
		map[string]interface{}{
			"unique_id":   "vallox_default_fan_speed",
			"name":        "Peruspuhallinnopeus",
			"device":      device,
			"icon":        "mdi:fan",
			"state_topic": topicFanDefaultSpeed,
		},
		map[string]interface{}{
			"unique_id":   "vallox_service_reminder_counter",
			"name":        "Huoltomuistuttimen kuukausilaskuri",
			"device":      device,
			"state_topic": topicServiceReminderCounter,
		},
		map[string]interface{}{
			"unique_id":   "vallox_rh_base",
			"name":        "Peruskosteustaso",
			"device":      device,
			"state_topic": topicRHBasic,
		},
		map[string]interface{}{
			"unique_id":    "vallox_cell_bypass_temp",
			"name":         "Kennonohituksen toimintalämpötila",
			"device":       device,
			"device_class": "temperature",
			"state_topic":  topicBypassOperating,
		},
		map[string]interface{}{
			"unique_id":   "vallox_supply_fan_control_setpoint",
			"name":        "Tasaviratuloilmapuhaltimen säädön asetusarvo",
			"device":      device,
			"state_topic": topicSupplyFanControlSetpoint,
		},
		map[string]interface{}{
			"unique_id":   "vallox_exhaust_fan_control_setpoint",
			"name":        "Tasavirtapoistoilmapuhaltimen säädön asetusarvo",
			"device":      device,
			"state_topic": topicExhaustFanControlSetpoint,
		},
		map[string]interface{}{
			"unique_id":   "vallox_cell_antifreeze_hysteresis",
			"name":        "Kennon jäätymiseneston lämpötilojen hystereesi",
			"device":      device,
			"state_topic": topicCellAntifreezeHysteresis,
		},
	},
	"number": {
		map[string]interface{}{
			"unique_id":     "vallox_current_fan_speed",
			"name":          "Nykyinen puhallinnopeus",
			"device":        device,
			"icon":          "mdi:fan",
			"state_topic":   topicFanCurrentSpeed,
			"command_topic": topicFanCurrentSpeed + "/set",
			"min":           1,
			"max":           8,
			"mode":          "slider",
		},
	},
}

type Config struct {
	SerialDevice string `envconfig:"serial_device" required:"true"`
	MqttUrl      string `envconfig:"mqtt_url" required:"true"`
	MqttUser     string `envconfig:"mqtt_user"`
	MqttPwd      string `envconfig:"mqtt_password"`
	MqttClientId string `envconfig:"mqtt_client_id" default:"vallox"`
	Debug        bool   `envconfig:"debug" default:"false"`
	EnableWrite  bool   `envconfig:"enable_write" default:"false"`
	EnableRaw    bool   `envconfig:"enable_raw" default:"false"`
}

var (
	config Config

	logDebug *log.Logger
	logInfo  *log.Logger
	logError *log.Logger

	updateSpeed          byte
	updateSpeedRequested time.Time
	currentSpeed         byte
	currentSpeedUpdated  time.Time

	speedUpdateRequest = make(chan byte, 10)
	speedUpdateSend    = make(chan byte, 10)

	homeassistantStatus = make(chan string, 10)
)

func init() {

	err := envconfig.Process("vallox", &config)
	if err != nil {
		log.Fatal(err.Error())
	}

	initLogging()
}

func main() {

	mqtt := connectMqtt()

	cache := make(map[byte]cacheEntry)

	announceMeToMqttDiscovery(mqtt, cache)

	valloxDevice := connectVallox()

	for {
		select {
		case event := <-valloxDevice.Events():
			handleValloxEvent(valloxDevice, event, cache, mqtt)
		case request := <-speedUpdateRequest:
			if hasSameRecentSpeed(request) {
				continue
			}
			updateSpeed = request
			updateSpeedRequested = time.Now()
			speedUpdateSend <- request
		case <-speedUpdateSend:
			sendSpeed(valloxDevice)
		case status := <-homeassistantStatus:
			if status == "online" {
				// HA became online, send discovery so it knows about entities
				go announceMeToMqttDiscovery(mqtt, cache)
			} else if status != "offline" {
				logInfo.Printf("unknown HA status message %s", status)
			}
		}
	}
}

func handleValloxEvent(valloxDev *vallox.Vallox, e vallox.Event, cache map[byte]cacheEntry, mqtt mqttClient.Client) {
	if !valloxDev.ForMe(e) {
		return // Ignore values not addressed for me
	}

	val, ok := cache[e.Register]
	if ok && val.value.RawValue == e.RawValue && time.Since(val.time) < time.Duration(15)*time.Minute {
		// Some values are not published by the device, so manually republish to keep the device online
		resendOldValues(valloxDev, mqtt, cache)
		// we already have that value and have recently published it, no need to publish to mqtt
		return
	}

	cached := cacheEntry{time: time.Now(), value: e}
	cache[e.Register] = cached

	if e.Register == vallox.RegisterCurrentFanSpeed {
		currentSpeed = byte(e.Value.(int16))
		currentSpeedUpdated = cached.time
	}

	go publishValue(mqtt, cached.value)
}

func sendSpeed(valloxDevice *vallox.Vallox) {
	if time.Since(updateSpeedRequested) < time.Duration(5)*time.Second {
		// Less than second old, retry later
		go func() {
			time.Sleep(time.Duration(1000) * time.Millisecond)
			speedUpdateSend <- updateSpeed
		}()
	} else if currentSpeed != updateSpeed || time.Since(currentSpeedUpdated) > 10*time.Second {
		logDebug.Printf("sending speed update to %x", updateSpeed)
		currentSpeed = updateSpeed
		currentSpeedUpdated = time.Now()
		valloxDevice.SetSpeed(updateSpeed)
		time.Sleep(time.Duration(20) * time.Millisecond)
		valloxDevice.Query(vallox.RegisterCurrentFanSpeed)
	}
}

func hasSameRecentSpeed(request byte) bool {
	return currentSpeed == request && time.Since(currentSpeedUpdated) < time.Duration(10)*time.Second
}

func connectVallox() *vallox.Vallox {
	cfg := vallox.Config{Device: config.SerialDevice, EnableWrite: config.EnableWrite, LogDebug: logDebug}

	logInfo.Printf("connecting to vallox serial port %s write enabled: %v", cfg.Device, cfg.EnableWrite)

	valloxDevice, err := vallox.Open(cfg)

	if err != nil {
		logError.Fatalf("error opening Vallox device %s: %v", config.SerialDevice, err)
	}

	return valloxDevice
}

func connectMqtt() mqttClient.Client {

	opts := mqttClient.NewClientOptions().
		AddBroker(config.MqttUrl).
		SetClientID(config.MqttClientId).
		SetOrderMatters(false).
		SetKeepAlive(150 * time.Second).
		SetAutoReconnect(true).
		SetConnectionLostHandler(connectionLostHandler).
		SetOnConnectHandler(connectHandler).
		SetReconnectingHandler(reconnectHandler)

	if len(config.MqttUser) > 0 {
		opts = opts.SetUsername(config.MqttUser)
	}

	if len(config.MqttPwd) > 0 {
		opts = opts.SetPassword(config.MqttPwd)
	}

	logInfo.Printf("connecting to mqtt %s client id %s user %s", opts.Servers, opts.ClientID, opts.Username)

	c := mqttClient.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	return c
}

func changeSpeedMessage(mqtt mqttClient.Client, msg mqttClient.Message) {
	body := string(msg.Payload())
	topic := msg.Topic()
	logInfo.Printf("received speed change %s to %s", body, topic)
	spd, err := strconv.ParseInt(body, 0, 8)
	if err != nil {
		logError.Printf("cannot parse speed from body %s", body)
	} else {
		speedUpdateRequest <- byte(spd)
	}
}

func haStatusMessage(mqtt mqttClient.Client, msg mqttClient.Message) {
	body := string(msg.Payload())
	homeassistantStatus <- body
}

func subscribe(mqtt mqttClient.Client) {
	logDebug.Print("subscribing to topics")
	mqtt.Subscribe("homeassistant/status", 0, haStatusMessage)
	mqtt.Subscribe("vallox/fan/currentSpeed/set", 0, changeSpeedMessage)
	// TODO: Other set topics
}

func resendOldValues(device *vallox.Vallox, mqtt mqttClient.Client, cache map[byte]cacheEntry) {
	// Speed is not automatically published by Vallox, so manually refresh the value
	now := time.Now()
	validTime := now.Add(time.Duration(-15) * time.Minute)
	if cached, ok := cache[vallox.RegisterCurrentFanSpeed]; ok && cached.time.Before(validTime) {
		device.Query(vallox.RegisterCurrentFanSpeed)
	}
}

func publishValue(mqtt mqttClient.Client, event vallox.Event) {

	if topic, ok := topicMap[event.Register]; ok {

		publish(mqtt, topic, fmt.Sprint(event.Value))
	}

	if registerFlags, ok := topicFlagMap[event.Register]; ok {
		for flag, topic := range registerFlags {
			publish(mqtt, topic, fmt.Sprint(event.RawValue&flag == flag))
		}
	}

	if config.EnableRaw {
		publish(mqtt, fmt.Sprintf("vallox/raw/%x", event.Register), fmt.Sprintf("%d", event.RawValue))
	}
}

func publish(mqtt mqttClient.Client, topic string, msg interface{}) {
	logDebug.Printf("publishing to %s msg %s", msg, topic)

	t := mqtt.Publish(topic, 0, false, msg)
	go func() {
		_ = t.Wait()
		if t.Error() != nil {
			logError.Printf("publishing msg failed %v", t.Error())
		}
	}()
}

func announceMeToMqttDiscovery(mqtt mqttClient.Client, cache map[byte]cacheEntry) {
	for key, entries := range discovery {
		for _, msg := range entries {
			jsonmsg, err := json.Marshal(msg)
			if err != nil {
				logError.Printf("Cannot marshal json %v", err)
				continue
			}
			publish(mqtt, fmt.Sprintf("homeassistant/%s/%s/config", key, msg["unique_id"].(string)), jsonmsg)
		}
	}
}

func connectionLostHandler(client mqttClient.Client, err error) {
	options := client.OptionsReader()
	logError.Printf("MQTT connection to %s lost %v", options.Servers(), err)
}

func connectHandler(client mqttClient.Client) {
	options := client.OptionsReader()
	logInfo.Printf("MQTT connected to %s", options.Servers())
	subscribe(client)
}

func reconnectHandler(client mqttClient.Client, options *mqttClient.ClientOptions) {
	logInfo.Printf("MQTT reconnecting to %s", options.Servers)
}

func initLogging() {
	writer := os.Stdout
	err := os.Stderr

	if config.Debug {
		logDebug = log.New(writer, "DEBUG ", log.Ldate|log.Ltime|log.Lmsgprefix)
	} else {
		logDebug = log.New(ioutil.Discard, "DEBUG ", 0)
	}
	logInfo = log.New(writer, "INFO  ", log.Ldate|log.Ltime|log.Lmsgprefix)
	logError = log.New(err, "ERROR ", log.Ldate|log.Ltime|log.Lmsgprefix)
}
