add_library(
        actNetHttp
        ws_common.cc
        ws_common.h
)
add_library(act::net::http ALIAS actNetHttp)
target_include_directories(
        actNetHttp PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actNetHttp PUBLIC
)
target_link_libraries(
        actNetHttp PRIVATE
        absl::failure_signal_handler
        absl::flags
        absl::flags_parse
        absl::log
        cppack
        act::concurrency
        act::nodes
        act::stores
)

add_subdirectory(proxygen)
add_subdirectory(tests)

# ------------------------------------------------------------------

add_library(
        actNetHttpWebsocketsPybind11 STATIC
)
add_library(
        act::net::http::websockets::pybind11 ALIAS
        actNetHttpWebsocketsPybind11
)
target_sources(
        actNetHttpWebsocketsPybind11 PRIVATE
        websockets_pybind11.cc
)
target_sources(
        actNetHttpWebsocketsPybind11 PUBLIC
        websockets_pybind11.h
)
target_link_libraries(
        actNetHttpWebsocketsPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_link_libraries(
        actNetHttpWebsocketsPybind11 PRIVATE
        act::concurrency
        act::net::http
        act::net::http::proxygen
        act::net::http::proxygen::folly_utils
)
target_include_directories(
        actNetHttpWebsocketsPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
)