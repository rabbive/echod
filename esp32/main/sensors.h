/**
 * @file sensors.h
 * @brief DHT22 temperature / humidity sensor driver for ESP32.
 *
 * Uses a bit-banged single-wire protocol on the configured GPIO pin.
 */

#ifndef SENSORS_H
#define SENSORS_H

#include <stdbool.h>

typedef struct {
    float temperature;  /* degrees Celsius */
    float humidity;     /* relative humidity % */
    bool  valid;        /* false if the read failed or CRC mismatch */
} sensor_reading_t;

/**
 * Initialise the DHT22 GPIO pin.  Call once at startup.
 *
 * @param gpio_num  GPIO number connected to the DHT22 DATA line.
 */
void sensors_init(int gpio_num);

/**
 * Read temperature and humidity from the DHT22.
 * Blocks for ~25 ms while the sensor responds.
 */
sensor_reading_t sensors_read(void);

#endif /* SENSORS_H */
