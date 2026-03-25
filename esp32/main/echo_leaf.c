/**
 * @file echo_leaf.c
 * @brief ECHO leaf-node state machine.
 *
 * Handles registration with a coordinator, sensor-data forwarding,
 * and coordinator-timeout detection.
 */

#include "echo_leaf.h"
#include "transport.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cJSON.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "sdkconfig.h"

static const char *TAG = "echo_leaf";

/* Forward declarations for the external MQTT handle. */
extern esp_mqtt_client_handle_t g_mqtt_client;

/* -------------------------------------------------------------- init */

void echo_leaf_init(echo_leaf_ctx_t *ctx, const char *node_id,
                    const char *cluster_id)
{
    memset(ctx, 0, sizeof(*ctx));
    strncpy(ctx->node_id, node_id, sizeof(ctx->node_id) - 1);
    strncpy(ctx->cluster_id, cluster_id, sizeof(ctx->cluster_id) - 1);
    ctx->state = LEAF_UNREGISTERED;
    ctx->coordinator_timeout_ms = CONFIG_ECHO_SENSOR_INTERVAL_MS * 20;
    ctx->last_coordinator_contact_us = esp_timer_get_time();
}

/* ---------------------------------------------------------- dispatch */

void echo_leaf_on_message(echo_leaf_ctx_t *ctx, const char *payload,
                          int payload_len)
{
    cJSON *root = cJSON_ParseWithLength(payload, payload_len);
    if (!root) return;

    const cJSON *sender   = cJSON_GetObjectItem(root, "sender_id");
    const cJSON *msg_type = cJSON_GetObjectItem(root, "msg_type");
    const cJSON *pl       = cJSON_GetObjectItem(root, "payload");

    if (!msg_type || !cJSON_IsString(msg_type)) {
        cJSON_Delete(root);
        return;
    }

    const char *type = msg_type->valuestring;

    if (strcmp(type, "leaf_register_response") == 0 && pl) {
        const cJSON *accepted = cJSON_GetObjectItem(pl, "accepted");
        const cJSON *coord    = cJSON_GetObjectItem(pl, "coordinator_id");
        if (cJSON_IsTrue(accepted) && coord && cJSON_IsString(coord)) {
            strncpy(ctx->coordinator_id, coord->valuestring,
                    sizeof(ctx->coordinator_id) - 1);
            ctx->state = LEAF_ACTIVE;
            ctx->last_coordinator_contact_us = esp_timer_get_time();
            ESP_LOGI(TAG, "%s registered with %s", ctx->node_id,
                     ctx->coordinator_id);
        }
    } else if (strcmp(type, "liveness_ping") == 0) {
        if (ctx->state == LEAF_ACTIVE) {
            ctx->last_coordinator_contact_us = esp_timer_get_time();
        }
    } else if (strcmp(type, "append_entries") == 0) {
        if (ctx->state == LEAF_ACTIVE) {
            ctx->last_coordinator_contact_us = esp_timer_get_time();
        }
    }

    cJSON_Delete(root);
}

/* -------------------------------------------------------------- tick */

void echo_leaf_tick(echo_leaf_ctx_t *ctx)
{
    if (ctx->state == LEAF_UNREGISTERED || ctx->state == LEAF_SEARCHING) {
        echo_leaf_send_register(ctx);
        return;
    }

    if (ctx->state == LEAF_ACTIVE) {
        int64_t elapsed_us =
            esp_timer_get_time() - ctx->last_coordinator_contact_us;
        if (elapsed_us > (int64_t)ctx->coordinator_timeout_ms * 1000) {
            ESP_LOGW(TAG, "%s coordinator timeout — SEARCHING",
                     ctx->node_id);
            ctx->state = LEAF_SEARCHING;
            ctx->coordinator_id[0] = '\0';
        }
    }
}

/* -------------------------------------------------------- registration */

void echo_leaf_send_register(echo_leaf_ctx_t *ctx)
{
    char payload[256];
    snprintf(payload, sizeof(payload),
             "{\"leaf_id\":\"%s\",\"capabilities\":{\"sensor\":\"temperature\"}}",
             ctx->node_id);

    /*
     * Pick a coordinator to register with.  In production a discovery
     * mechanism would be used; for now we target "coord-0".
     */
    const char *target = "coord-0";
    transport_send(g_mqtt_client, ctx->cluster_id, ctx->node_id,
                   target, "leaf_register", payload);

    ESP_LOGI(TAG, "%s sent registration to %s", ctx->node_id, target);
}

/* --------------------------------------------------------- sensor data */

void echo_leaf_send_sensor(echo_leaf_ctx_t *ctx, const char *sensor_type,
                           float value)
{
    if (ctx->state != LEAF_ACTIVE || ctx->coordinator_id[0] == '\0')
        return;

    char payload[256];
    snprintf(payload, sizeof(payload),
             "{\"leaf_id\":\"%s\",\"sensor_type\":\"%s\",\"value\":%.2f}",
             ctx->node_id, sensor_type, value);

    transport_send(g_mqtt_client, ctx->cluster_id, ctx->node_id,
                   ctx->coordinator_id, "sensor_data", payload);
}
