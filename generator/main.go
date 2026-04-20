package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type TelemetryPayload struct {
	Time          string  `json:"time"`
	DeviceID      string  `json:"device_id"`
	SolarYieldKW  float64 `json:"solar_yield_kw"`
	BatterySOCPct float64 `json:"battery_soc_pct"`
}

func simulateDevice(deviceID string, brokerURL string) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(deviceID)
	client := mqtt.NewClient(opts)

	// connect to mosquitto broker (with retry loop)
	for {
		if token := client.Connect(); token.Wait() && token.Error() == nil {
			fmt.Printf("🔌 [%s] Connected to Mosquitto MQTT Broker!\n", deviceID)
			break
		}
		fmt.Printf("⚠️ [%s] Waiting for broker at %s...\n", deviceID, brokerURL)
		time.Sleep(2 * time.Second)
	}

	topic := fmt.Sprintf("edp/telemetry/%s", deviceID)
	// infinity telemetry loop
	for {
		solar := math.Round((rand.Float64()*40.0+10.0)*100) / 100
		battery := math.Round((rand.Float64()*80.0+20.0)*100) / 100

		payload := TelemetryPayload{
			Time:          time.Now().UTC().Format(time.RFC3339),
			DeviceID:      deviceID,
			SolarYieldKW:  solar,
			BatterySOCPct: battery,
		}

		jsonData, _ := json.Marshal(payload)

		// qos 1 guarantees delivery at least once
		token := client.Publish(topic, 1, false, jsonData)
		token.Wait()

		fmt.Printf("📡 [%s] Published to %s\n", deviceID, topic)

		time.Sleep(5 * time.Second)
	}
}

func main() {
	fmt.Println("🚀 Starting EDP Edge MQTT Generator...")

	// 1. Read from the environment, fallback to localhost for local testing
	brokerURL := os.Getenv("MQTT_BROKER_URL")
	if brokerURL == "" {
		brokerURL = "tcp://localhost:1883"
	}

	fmt.Printf("📡 Connecting to MQTT Broker at: %s\n", brokerURL)

	devices := []string{"factory-roof-01", "warehouse-solar-02", "office-battery-03"}

	for _, device := range devices {
		go simulateDevice(device, brokerURL)
	}

	select {}
}
