include(CMakeFindDependencyMacro)

find_dependency(aws-c-io)

if (@MQTT_WITH_WEBSOCKETS@)
    find_dependency(aws-c-http)
endif()

if (BUILD_SHARED_LIBS)
    include(${CMAKE_CURRENT_LIST_DIR}/shared/@CMAKE_PROJECT_NAME@-targets.cmake)
else()
    include(${CMAKE_CURRENT_LIST_DIR}/static/@CMAKE_PROJECT_NAME@-targets.cmake)
endif()

