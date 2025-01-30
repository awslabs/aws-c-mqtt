project(elastishadow C)

file(GLOB ELASTISHADOW_SRC
        "*.c"
        )

set(ELASTISHADOW_PROJECT_NAME elastishadow)
add_executable(${ELASTISHADOW_PROJECT_NAME} ${ELASTISHADOW_SRC})
aws_set_common_properties(${ELASTISHADOW_PROJECT_NAME})


target_include_directories(${ELASTISHADOW_PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(${ELASTISHADOW_PROJECT_NAME} PRIVATE aws-c-mqtt)

if (BUILD_SHARED_LIBS AND NOT WIN32)
    message(INFO " elastishadow will be built with shared libs, but you may need to set LD_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib to run the application")
endif()

install(TARGETS ${ELASTISHADOW_PROJECT_NAME}
        EXPORT ${ELASTISHADOW_PROJECT_NAME}-targets
        COMPONENT Runtime
        RUNTIME
        DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT Runtime)
