/**
 * @file transport.c
 * @brief MQTT transport — ESP-MQTT wrapper for ECHO leaf nodes.
 */

#include "transport.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

#include "esp_event.h"
#include "esp_log.h"
#include "sdkconfig.h"

static const char *TAG = "transport";

/* ------------------------------------------------------------------ state */

static transport_msg_cb_t s_msg_cb = NULL;
static char s_rpc_topic[128];
static char s_broadcast_topic[128];

/* ---------------------------------------------------------- MQTT handler */

static void mqtt_event_handler(void *arg, esp_event_base_t base,
                                int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;

    switch (event->event_id) {
    case MQTT_EVENT_CONNECTED:
        ESP_LOGI(TAG, "Connected to broker");
        esp_mqtt_client_subscribe(event->client, s_rpc_topic, 0);
        esp_mqtt_client_subscribe(event->client, s_broadcast_topic, 0);
        break;

    case MQTT_EVENT_DATA:
        if (s_msg_cb && event->data_len > 0) {
            char *buf = calloc(1, event->data_len + 1);
            if (buf) {
                memcpy(buf, event->data, event->data_len);
                char *topic_buf = calloc(1, event->topic_len + 1);
                if (topic_buf) {
                    memcpy(topic_buf, event->topic, event->topic_len);
                    s_msg_cb(topic_buf, buf, event->data_len);
                    free(topic_buf);
                }
                free(buf);
            }
        }
        break;

    case MQTT_EVENT_DISCONNECTED:
        ESP_LOGW(TAG, "Disconnected from broker");
        break;

    default:
        break;
    }
}

/* -------------------------------------------------------------- public */

esp_mqtt_client_handle_t transport_init(const char *broker_uri,
                                        const char *node_id,
                                        const char *cluster_id,
                                        transport_msg_cb_t cb)
{
    s_msg_cb = cb;

    snprintf(s_rpc_topic, sizeof(s_rpc_topic),
             "echo/%s/rpc/%s", cluster_id, node_id);
    snprintf(s_broadcast_topic, sizeof(s_broadcast_topic),
             "echo/%s/broadcast", cluster_id);

    const esp_mqtt_client_config_t cfg = {
        .broker.uri = broker_uri,
    };

    esp_mqtt_client_handle_t client = esp_mqtt_client_init(&cfg);
    esp_mqtt_client_register_event(client, ESP_EVENT_ANY_ID,
                                   mqtt_event_handler, NULL);
    esp_mqtt_client_start(client);
    return client;
}

void transport_send(esp_mqtt_client_handle_t client,
                    const char *cluster_id,
                    const char *sender_id,
                    const char *recipient_id,
                    const char *msg_type,
                    const char *payload_json)
{
    char topic[128];
    snprintf(topic, sizeof(topic), "echo/%s/rpc/%s", cluster_id, recipient_id);

    char envelope[512];
    snprintf(envelope, sizeof(envelope),
             "{\"sender_id\":\"%s\",\"recipient_id\":\"%s\","
             "\"msg_type\":\"%s\",\"payload\":%s,\"timestamp\":%ld}",
             sender_id, recipient_id, msg_type, payload_json,
             (long)time(NULL));

    esp_mqtt_client_publish(client, topic, envelope, 0, 0, 0);
}

void transport_broadcast(esp_mqtt_client_handle_t client,
                         const char *cluster_id,
                         const char *sender_id,
                         const char *msg_type,
                         const char *payload_json)
{
    char topic[128];
    snprintf(topic, sizeof(topic), "echo/%s/broadcast", cluster_id);

    char envelope[512];
    snprintf(envelope, sizeof(envelope),
             "{\"sender_id\":\"%s\",\"recipient_id\":\"*\","
             "\"msg_type\":\"%s\",\"payload\":%s,\"timestamp\":%ld}",
             sender_id, msg_type, payload_json, (long)time(NULL));

    esp_mqtt_client_publish(client, topic, envelope, 0, 0, 0);
}
