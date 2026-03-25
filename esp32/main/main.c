/**
 * @file main.c
 * @brief Entry point for the ECHO leaf node firmware (ESP32 / ESP-IDF).
 *
 * Boot sequence:
 *   1. Initialise NVS flash
 *   2. Connect to WiFi
 *   3. Connect to the MQTT broker
 *   4. Initialise the DHT22 sensor
 *   5. Enter the main loop — read sensor, forward to coordinator
 */

#include <math.h>
#include <stdio.h>
#include <string.h>

#include "echo_leaf.h"
#include "sensors.h"
#include "transport.h"

#include "esp_event.h"
#include "esp_log.h"
#include "esp_wifi.h"
#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"
#include "freertos/task.h"
#include "nvs_flash.h"
#include "sdkconfig.h"

static const char *TAG = "main";

/* Global MQTT handle used by echo_leaf.c */
esp_mqtt_client_handle_t g_mqtt_client = NULL;

/* WiFi event group */
static EventGroupHandle_t s_wifi_event_group;
#define WIFI_CONNECTED_BIT BIT0

/* -------------------------------------------------------------- WiFi */

static void wifi_event_handler(void *arg, esp_event_base_t base,
                                int32_t id, void *data)
{
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        esp_wifi_connect();
        ESP_LOGW(TAG, "WiFi disconnected — reconnecting");
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)data;
        ESP_LOGI(TAG, "Got IP: " IPSTR, IP2STR(&event->ip_info.ip));
        xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
    }
}

static void wifi_init(void)
{
    s_wifi_event_group = xEventGroupCreate();

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    esp_event_handler_instance_t inst_any, inst_got_ip;
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, &inst_any));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, &inst_got_ip));

    wifi_config_t wifi_cfg = {
        .sta = {
            .ssid     = CONFIG_ECHO_WIFI_SSID,
            .password = CONFIG_ECHO_WIFI_PASSWORD,
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_cfg));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGI(TAG, "Waiting for WiFi connection…");
    xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT,
                        pdFALSE, pdFALSE, portMAX_DELAY);
}

/* -------------------------------------------------------- MQTT callback */

static echo_leaf_ctx_t s_leaf;

static void on_mqtt_msg(const char *topic, const char *data, int data_len)
{
    echo_leaf_on_message(&s_leaf, data, data_len);
}

/* -------------------------------------------------------------- main */

void app_main(void)
{
    ESP_ERROR_CHECK(nvs_flash_init());
    wifi_init();

    /* Leaf context */
    echo_leaf_init(&s_leaf, CONFIG_ECHO_NODE_ID, CONFIG_ECHO_CLUSTER_ID);

    /* MQTT */
    g_mqtt_client = transport_init(
        CONFIG_ECHO_MQTT_BROKER_URI,
        CONFIG_ECHO_NODE_ID,
        CONFIG_ECHO_CLUSTER_ID,
        on_mqtt_msg);

    /* Allow MQTT to connect before first registration attempt */
    vTaskDelay(pdMS_TO_TICKS(2000));

    /* DHT22 sensor */
    sensors_init(CONFIG_ECHO_DHT_GPIO);

    float delta_threshold = CONFIG_ECHO_DELTA_THRESHOLD / 100.0f;
    float last_sent_temp = 0.0f;
    bool first_reading = true;

    ESP_LOGI(TAG, "Entering main loop (interval=%d ms, delta=%.2f)",
             CONFIG_ECHO_SENSOR_INTERVAL_MS, delta_threshold);

    while (1) {
        echo_leaf_tick(&s_leaf);

        sensor_reading_t reading = sensors_read();
        if (reading.valid) {
            float diff = fabsf(reading.temperature - last_sent_temp);
            float denom = fabsf(last_sent_temp) > 0.001f
                              ? fabsf(last_sent_temp)
                              : 1.0f;

            if (first_reading || (diff / denom) >= delta_threshold) {
                echo_leaf_send_sensor(&s_leaf, "temperature",
                                      reading.temperature);
                last_sent_temp = reading.temperature;
                first_reading = false;
                ESP_LOGI(TAG, "Sent temperature=%.1f°C humidity=%.1f%%",
                         reading.temperature, reading.humidity);
            }
        }

        vTaskDelay(pdMS_TO_TICKS(CONFIG_ECHO_SENSOR_INTERVAL_MS));
    }
}
