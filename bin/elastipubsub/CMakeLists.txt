project(elastipubsub C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")

file(GLOB ELASTIPUBSUB_SRC
        "*.c"
        )

set(ELASTIPUBSUB_PROJECT_NAME elastipubsub)
add_executable(${ELASTIPUBSUB_PROJECT_NAME} ${ELASTIPUBSUB_SRC})
aws_set_common_properties(${ELASTIPUBSUB_PROJECT_NAME})


target_include_directories(${ELASTIPUBSUB_PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(${ELASTIPUBSUB_PROJECT_NAME} aws-c-mqtt)

if (BUILD_SHARED_LIBS AND NOT WIN32)
    message(INFO " elastiPUBSUB will be built with shared libs, but you may need to set LD_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib to run the application")
endif()

install(TARGETS ${ELASTIPUBSUB_PROJECT_NAME}
        EXPORT ${ELASTIPUBSUB_PROJECT_NAME}-targets
        COMPONENT Runtime
        RUNTIME
        DESTINATION bin
        COMPONENT Runtime)
