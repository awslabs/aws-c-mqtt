project(mqtt3_client_app C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")

file(GLOB MQTT3_CLIENT_APP_SRC
        "*.c"
        )

set(MQTT3_CLIENT_APP_PROJECT_NAME mqtt3_client_app)
add_executable(${MQTT3_CLIENT_APP_PROJECT_NAME} ${MQTT3_CLIENT_APP_SRC})
aws_set_common_properties(${MQTT3_CLIENT_APP_PROJECT_NAME})

target_include_directories(${MQTT3_CLIENT_APP_PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(${MQTT3_CLIENT_APP_PROJECT_NAME} PRIVATE aws-c-mqtt)

if (BUILD_SHARED_LIBS AND NOT WIN32)
    message(INFO " mqtt3_client_app will be built with shared libs, but you may need to set LD_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib to run the application")
endif()

install(TARGETS ${MQTT3_CLIENT_APP_PROJECT_NAME}
        EXPORT ${MQTT3_CLIENT_APP_PROJECT_NAME}-targets
        COMPONENT Runtime
        RUNTIME
        DESTINATION bin
        COMPONENT Runtime)