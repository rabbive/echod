/**
 * @file echo_leaf.h
 * @brief ECHO leaf-node state machine for ESP32.
 *
 * States: UNREGISTERED -> ACTIVE -> SEARCHING
 * The leaf registers with a coordinator, streams sensor data,
 * and passively receives committed log entries.
 */

#ifndef ECHO_LEAF_H
#define ECHO_LEAF_H

#include <stdbool.h>
#include <stdint.h>

typedef enum {
    LEAF_UNREGISTERED = 0,
    LEAF_ACTIVE,
    LEAF_SEARCHING,
} echo_leaf_state_t;

typedef struct {
    char node_id[32];
    char cluster_id[32];
    char coordinator_id[32];
    echo_leaf_state_t state;
    float last_committed_value;
    uint32_t coordinator_timeout_ms;
    int64_t last_coordinator_contact_us;
} echo_leaf_ctx_t;

/**
 * Initialise the leaf context.
 */
void echo_leaf_init(echo_leaf_ctx_t *ctx, const char *node_id,
                    const char *cluster_id);

/**
 * Called when an MQTT message arrives on the node's RPC topic.
 * Dispatches to the appropriate handler based on msg_type.
 */
void echo_leaf_on_message(echo_leaf_ctx_t *ctx, const char *payload,
                          int payload_len);

/**
 * Periodic tick — checks coordinator timeout and drives state
 * transitions.  Call from the main loop at ~100 ms intervals.
 */
void echo_leaf_tick(echo_leaf_ctx_t *ctx);

/**
 * Attempt to register with a coordinator by publishing a
 * leaf_register message.
 */
void echo_leaf_send_register(echo_leaf_ctx_t *ctx);

/**
 * Send a sensor reading to the registered coordinator.
 */
void echo_leaf_send_sensor(echo_leaf_ctx_t *ctx, const char *sensor_type,
                           float value);

#endif /* ECHO_LEAF_H */
