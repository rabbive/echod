/**
 * @file sensors.c
 * @brief DHT22 bit-banged driver for ESP32.
 *
 * Protocol overview (single-wire, 40-bit response):
 *   Host pulls DATA low for >= 1 ms, then releases.
 *   DHT22 responds with 80 us low + 80 us high, then 40 data bits
 *   (each bit: 50 us low + 26-28 us high = 0, 70 us high = 1).
 *   Data: 16-bit humidity, 16-bit temperature, 8-bit checksum.
 */

#include "sensors.h"

#include <string.h>

#include "driver/gpio.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "rom/ets_sys.h"

static const char *TAG = "sensors";
static int s_gpio = -1;

/* -------------------------------------------------------------- helpers */

static int wait_level(int level, int timeout_us)
{
    int64_t start = esp_timer_get_time();
    while (gpio_get_level(s_gpio) == level) {
        if ((esp_timer_get_time() - start) > timeout_us)
            return -1;
    }
    return (int)(esp_timer_get_time() - start);
}

/* -------------------------------------------------------------- public */

void sensors_init(int gpio_num)
{
    s_gpio = gpio_num;
    gpio_reset_pin(gpio_num);
    ESP_LOGI(TAG, "DHT22 initialised on GPIO %d", gpio_num);
}

sensor_reading_t sensors_read(void)
{
    sensor_reading_t result = { .valid = false };
    uint8_t data[5] = {0};

    /* --- Start signal: pull low >= 1 ms, then release --- */
    gpio_set_direction(s_gpio, GPIO_MODE_OUTPUT);
    gpio_set_level(s_gpio, 0);
    ets_delay_us(1200);
    gpio_set_level(s_gpio, 1);
    ets_delay_us(30);
    gpio_set_direction(s_gpio, GPIO_MODE_INPUT);

    /* --- Wait for DHT response (80 us low + 80 us high) --- */
    if (wait_level(0, 100) < 0) return result;
    if (wait_level(1, 100) < 0) return result;

    /* --- Read 40 bits --- */
    for (int i = 0; i < 40; i++) {
        if (wait_level(0, 80) < 0) return result;
        int high_us = wait_level(1, 100);
        if (high_us < 0) return result;
        data[i / 8] <<= 1;
        if (high_us > 40)
            data[i / 8] |= 1;
    }

    /* --- Checksum --- */
    uint8_t sum = data[0] + data[1] + data[2] + data[3];
    if (sum != data[4]) {
        ESP_LOGW(TAG, "DHT22 checksum mismatch (got 0x%02x, expected 0x%02x)",
                 data[4], sum);
        return result;
    }

    uint16_t raw_hum  = ((uint16_t)data[0] << 8) | data[1];
    uint16_t raw_temp = ((uint16_t)data[2] << 8) | data[3];

    result.humidity    = raw_hum / 10.0f;
    result.temperature = (raw_temp & 0x7FFF) / 10.0f;
    if (raw_temp & 0x8000)
        result.temperature = -result.temperature;
    result.valid = true;

    return result;
}
