include(CMakeFindDependencyMacro)

find_dependency(aws-c-io)

if (@MQTT_WITH_WEBSOCKETS@)
    find_dependency(aws-c-http)
endif()

include(${CMAKE_CURRENT_LIST_DIR}/@CMAKE_PROJECT_NAME@-targets.cmake)
