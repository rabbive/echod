/**
 * @file transport.h
 * @brief MQTT transport wrapper for the ECHO leaf node.
 *
 * Provides connect / publish / subscribe helpers that map the ECHO
 * protocol topic layout onto the ESP-MQTT client.
 */

#ifndef TRANSPORT_H
#define TRANSPORT_H

#include "esp_mqtt_client.h"

typedef void (*transport_msg_cb_t)(const char *topic, const char *data,
                                   int data_len);

/**
 * Initialise and start the MQTT client.
 *
 * @param broker_uri  e.g. "mqtt://192.168.1.1:1883"
 * @param node_id     unique node identifier
 * @param cluster_id  logical cluster name
 * @param cb          callback invoked for every inbound message
 * @return the MQTT client handle
 */
esp_mqtt_client_handle_t transport_init(const char *broker_uri,
                                        const char *node_id,
                                        const char *cluster_id,
                                        transport_msg_cb_t cb);

/**
 * Publish a JSON envelope to a specific node's RPC topic.
 */
void transport_send(esp_mqtt_client_handle_t client,
                    const char *cluster_id,
                    const char *sender_id,
                    const char *recipient_id,
                    const char *msg_type,
                    const char *payload_json);

/**
 * Publish a JSON envelope to the cluster broadcast topic.
 */
void transport_broadcast(esp_mqtt_client_handle_t client,
                         const char *cluster_id,
                         const char *sender_id,
                         const char *msg_type,
                         const char *payload_json);

#endif /* TRANSPORT_H */
