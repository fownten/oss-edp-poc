package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type TelemetryPayload struct {
	Time          string   `json:"time"`
	FactoryID     string   `json:"factory_id"`
	DeviceID      string   `json:"device_id"`
	DeviceType    string   `json:"device_type"`
	SolarYieldKW  *float64 `json:"solar_yield_kw"`
	BatterySOCPct *float64 `json:"battery_soc_pct"`
}

func getEnvInt(key string, defaultVal int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		fmt.Printf("⚠️ Invalid value for env %s: %s. Using default: %d\n", key, valStr, defaultVal)
		return defaultVal
	}
	return val
}

func simulateSolarPanel(factoryID string, deviceID string, brokerURL string, intervalSec int) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(fmt.Sprintf("%s-%s", factoryID, deviceID))
	client := mqtt.NewClient(opts)

	// Connect loop
	for {
		if token := client.Connect(); token.Wait() && token.Error() == nil {
			fmt.Printf("🔌 [%s/%s] Connected to Mosquitto MQTT Broker!\n", factoryID, deviceID)
			break
		}
		fmt.Printf("⚠️ [%s/%s] Waiting for broker at %s...\n", factoryID, deviceID, brokerURL)
		time.Sleep(2 * time.Second)
	}

	topic := fmt.Sprintf("edp/telemetry/%s/%s", factoryID, deviceID)

	for {
		utcHour := time.Now().UTC().Hour()
		var solarVal float64
		// Solar active between 6 AM and 6 PM UTC
		if utcHour >= 6 && utcHour < 18 {
			rad := math.Pi * float64(utcHour-6) / 12.0
			// Peak generation around 40kW + some solar noise
			solarVal = (math.Sin(rad) * 40.0) + (rand.Float64() * 5.0)
			if solarVal < 0 {
				solarVal = 0
			}
		} else {
			solarVal = 0
		}
		solarVal = math.Round(solarVal*100) / 100

		payload := TelemetryPayload{
			Time:          time.Now().UTC().Format(time.RFC3339),
			FactoryID:     factoryID,
			DeviceID:      deviceID,
			DeviceType:    "solar",
			SolarYieldKW:  &solarVal,
			BatterySOCPct: nil, // null for solar panels
		}

		jsonData, _ := json.Marshal(payload)
		token := client.Publish(topic, 1, false, jsonData)
		token.Wait()

		fmt.Printf("📡 [%s/%s] Published Solar: %.2f kW\n", factoryID, deviceID, solarVal)
		time.Sleep(time.Duration(intervalSec) * time.Second)
	}
}

func simulateBattery(factoryID string, deviceID string, brokerURL string, intervalSec int) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(fmt.Sprintf("%s-%s", factoryID, deviceID))
	client := mqtt.NewClient(opts)

	// Connect loop
	for {
		if token := client.Connect(); token.Wait() && token.Error() == nil {
			fmt.Printf("🔌 [%s/%s] Connected to Mosquitto MQTT Broker!\n", factoryID, deviceID)
			break
		}
		fmt.Printf("⚠️ [%s/%s] Waiting for broker at %s...\n", factoryID, deviceID, brokerURL)
		time.Sleep(2 * time.Second)
	}

	topic := fmt.Sprintf("edp/telemetry/%s/%s", factoryID, deviceID)

	// Stateful battery SOC
	batterySOC := 50.0

	for {
		utcHour := time.Now().UTC().Hour()
		// Charge during daylight, discharge during the night
		var change float64
		if utcHour >= 6 && utcHour < 18 {
			// Charging: solar energy surplus
			change = rand.Float64() * 3.0
		} else {
			// Discharging: factory runs on battery
			change = -rand.Float64() * 2.5
		}

		batterySOC += change
		if batterySOC > 100.0 {
			batterySOC = 100.0
		} else if batterySOC < 10.0 {
			batterySOC = 10.0
		}
		batterySOC = math.Round(batterySOC*100) / 100

		payload := TelemetryPayload{
			Time:          time.Now().UTC().Format(time.RFC3339),
			FactoryID:     factoryID,
			DeviceID:      deviceID,
			DeviceType:    "battery",
			SolarYieldKW:  nil, // null for batteries
			BatterySOCPct: &batterySOC,
		}

		jsonData, _ := json.Marshal(payload)
		token := client.Publish(topic, 1, false, jsonData)
		token.Wait()

		fmt.Printf("📡 [%s/%s] Published Battery: %.2f%%\n", factoryID, deviceID, batterySOC)
		time.Sleep(time.Duration(intervalSec) * time.Second)
	}
}

func main() {
	fmt.Println("🚀 Starting EDP Factory MQTT Generator...")

	brokerURL := os.Getenv("MQTT_BROKER_URL")
	if brokerURL == "" {
		brokerURL = "tcp://localhost:1883"
	}

	factoryID := os.Getenv("FACTORY_ID")
	if factoryID == "" {
		factoryID = "factory-default"
	}

	solarCount := getEnvInt("SOLAR_PANELS_COUNT", 2)
	batteryCount := getEnvInt("BATTERIES_COUNT", 1)
	intervalSec := getEnvInt("GENERATION_INTERVAL_SEC", 5)

	fmt.Printf("🏭 Factory Configured: ID=%s, Solar Panels=%d, Batteries=%d, Interval=%ds\n",
		factoryID, solarCount, batteryCount, intervalSec)
	fmt.Printf("📡 Connecting to MQTT Broker at: %s\n", brokerURL)

	// Start Solar panel simulators
	for i := 1; i <= solarCount; i++ {
		deviceID := fmt.Sprintf("solar-%02d", i)
		go simulateSolarPanel(factoryID, deviceID, brokerURL, intervalSec)
	}

	// Start Battery simulators
	for i := 1; i <= batteryCount; i++ {
		deviceID := fmt.Sprintf("battery-%02d", i)
		go simulateBattery(factoryID, deviceID, brokerURL, intervalSec)
	}

	// Block main goroutine
	select {}
}
