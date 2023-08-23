project(mqtt5canary C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")

file(GLOB MQTT5CANARY_SRC
        "*.c"
        )

set(MQTT5CANARY_PROJECT_NAME mqtt5canary)
add_executable(${MQTT5CANARY_PROJECT_NAME} ${MQTT5CANARY_SRC})
aws_set_common_properties(${MQTT5CANARY_PROJECT_NAME})


target_include_directories(${MQTT5CANARY_PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(${MQTT5CANARY_PROJECT_NAME} PRIVATE aws-c-mqtt)

if (BUILD_SHARED_LIBS AND NOT WIN32)
    message(INFO " mqtt5canary will be built with shared libs, but you may need to set LD_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib to run the application")
endif()

install(TARGETS ${MQTT5CANARY_PROJECT_NAME}
        EXPORT ${MQTT5CANARY_PROJECT_NAME}-targets
        COMPONENT Runtime
        RUNTIME
        DESTINATION bin
        COMPONENT Runtime)
