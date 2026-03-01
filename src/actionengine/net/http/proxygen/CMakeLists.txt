if (ACTIONENGINE_USE_PROXYGEN)
    # To expose FindSodium.cmake:
    list(APPEND CMAKE_MODULE_PATH ${ACTIONENGINE_PROJECT_ROOT}/cmake)

    # Disable warnings about FindBoost's removal
    if (POLICY CMP0167)
        cmake_policy(SET CMP0167 NEW)
    endif ()

    set(proxygen_DIR ${ACTIONENGINE_THIRD_PARTY_ROOT}/proxygen/proxygen/_build/lib/cmake/proxygen)

    set(CMAKE_PREFIX_PATH
            ${CMAKE_PREFIX_PATH}
            ${ACTIONENGINE_PROXYGEN_ROOT}
            ${ACTIONENGINE_THIRD_PARTY_ROOT}/proxygen/proxygen/_build/deps
    )

    #        find_package(Sodium REQUIRED)
    #        find_package(gflags CONFIG REQUIRED)
    find_package(c-ares REQUIRED)
    find_package(proxygen REQUIRED)

    add_library(actNetHttpProxygenFollyUtils STATIC
            folly_utils.cc
            folly_utils.h
    )
    target_compile_definitions(actNetHttpProxygenFollyUtils PUBLIC "-DGLOG_USE_GLOG_EXPORT")

    add_library(act::net::http::proxygen::folly_utils ALIAS actNetHttpProxygenFollyUtils)

    target_link_libraries(
            actNetHttpProxygenFollyUtils PRIVATE
            act::concurrency
            absl::log
            Folly::folly
    )

    target_include_directories(
            actNetHttpProxygenFollyUtils PUBLIC
            ${ACTIONENGINE_PROJECT_TOP_LEVEL}
            ${ACTIONENGINE_INCLUDE_DIR}
            ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
    )

    add_library(actNetHttpProxygen STATIC
            http_connector.cc
            http_connector.h
            wire_stream.cc
            wire_stream.h
            ws_client_stream.cc
            ws_client_stream.h
            ws_server_stream.cc
            ws_server_stream.h
            server.cc
            server.h
            ws_server_stream_handler_factory.cc
            ws_server_stream_handler_factory.h
    )

    add_library(act::net::http::proxygen ALIAS actNetHttpProxygen)

    target_include_directories(
            actNetHttpProxygen PUBLIC
            ${ACTIONENGINE_PROJECT_TOP_LEVEL}
            ${ACTIONENGINE_INCLUDE_DIR}
            ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
    )

    target_link_libraries(actNetHttpProxygen PUBLIC
            act::net::http
    )

    target_link_libraries(
            actNetHttpProxygen PUBLIC
            absl::log
            c-ares::cares
            proxygen::proxygen
            proxygen::proxygenhttpserver
            cppack
            act::actions
            act::concurrency
            act::net::http::proxygen::folly_utils
            act::nodes
            act::service
            act::stores
    )
endif (ACTIONENGINE_USE_PROXYGEN)