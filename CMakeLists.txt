cmake_minimum_required(VERSION 3.1)
project(aws-c-mqtt C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")

include(AwsCFlags)
include(Sanitizers)
include(CheckCCompilerFlag)

file(GLOB AWS_MQTT_HEADERS
        "include/aws/mqtt/*.h"
        )

file(GLOB AWS_MQTT_PRIV_HEADERS
        "include/aws/mqtt/private/*.h"
        )

file(GLOB AWS_MQTT_SRC
        "source/*.c"
        )

file(GLOB MQTT_HEADERS
        ${AWS_MQTT_HEADERS}
        ${AWS_MQTT_PRIV_HEADERS}
        )

file(GLOB MQTT_SRC
        ${AWS_MQTT_SRC}
        )

add_library(${CMAKE_PROJECT_NAME} ${LIBTYPE} ${MQTT_HEADERS} ${MQTT_SRC})
aws_set_common_properties(${CMAKE_PROJECT_NAME})
aws_add_sanitizers(${CMAKE_PROJECT_NAME})

target_include_directories(${CMAKE_PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

if (UNIX AND NOT APPLE)
    find_package(s2n REQUIRED)
endif ()
find_package(aws-c-common REQUIRED)
find_package(aws-c-io REQUIRED)
target_link_libraries(${CMAKE_PROJECT_NAME} PUBLIC aws-c-io)

install(FILES ${AWS_MQTT_HEADERS} DESTINATION "include/aws/mqtt")
install(FILES ${AWS_MQTT_TESTING_HEADERS} DESTINATION "include/aws/testing/mqtt")

install(
        TARGETS ${CMAKE_PROJECT_NAME} EXPORT ${CMAKE_PROJECT_NAME}-config
        ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
        LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
        COMPONENT library
)

export(TARGETS ${CMAKE_PROJECT_NAME} FILE ${CMAKE_PROJECT_NAME}-config.cmake)
install(EXPORT ${CMAKE_PROJECT_NAME}-config DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/${CMAKE_PROJECT_NAME}/cmake/")

include(CTest)
add_subdirectory(tests)
