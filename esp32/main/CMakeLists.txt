idf_component_register(
    SRCS "main.c" "echo_leaf.c" "transport.c" "sensors.c"
    INCLUDE_DIRS "."
    REQUIRES mqtt esp_wifi esp_event nvs_flash driver
)
