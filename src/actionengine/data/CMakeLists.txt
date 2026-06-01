add_library(actData STATIC)
add_library(act::data ALIAS actData)

if (APPLE)
    execute_process(
            COMMAND brew --prefix icu4c
            OUTPUT_VARIABLE ICU_BREW_PREFIX
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    message("ICU_BREW_PREFIX " ${ICU_BREW_PREFIX})
    find_path(ICU_BREW_INCLUDE_DIR
            NAMES unicode/unistr.h
            PATHS "${ICU_BREW_PREFIX}/include"
            NO_DEFAULT_PATH
            NO_SYSTEM_ENVIRONMENT_PATH
            NO_CMAKE_FIND_ROOT_PATH
            NO_CACHE
    )
    message("ICU_BREW_INCLUDE_DIR " ${ICU_BREW_INCLUDE_DIR})
    find_library(ICU_UC_LIBRARY
            NAMES icuuc
            PATHS "${ICU_BREW_PREFIX}/lib"
            NO_DEFAULT_PATH
            NO_SYSTEM_ENVIRONMENT_PATH
            NO_CMAKE_FIND_ROOT_PATH
            NO_CACHE
            REQUIRED
    )
    find_library(ICU_I18N_LIBRARY
            NAMES icui18n
            PATHS "${ICU_BREW_PREFIX}/lib"
            NO_DEFAULT_PATH
            NO_SYSTEM_ENVIRONMENT_PATH
            NO_CMAKE_FIND_ROOT_PATH
            NO_CACHE
            REQUIRED
    )
    target_include_directories(actData PRIVATE ${ICU_BREW_INCLUDE_DIR})
    target_link_libraries(actData PUBLIC
            ${ICU_UC_LIBRARY}
            ${ICU_I18N_LIBRARY}
    )

else ()
    find_package(ICU COMPONENTS uc i18n REQUIRED)
    target_link_libraries(actData PUBLIC
            ICU::uc
            ICU::i18n
    )
endif (APPLE)

target_sources(actData
        PUBLIC
        conversion.h
        types.h
        mimetypes.h
        msgpack.h
        serialization.h
)
target_sources(actData
        PRIVATE
        msgpack.cc
        serialization.cc
        types.cc
)
target_include_directories(
        actData PUBLIC
        ${ACTIONENGINE_PROJECT_TOP_LEVEL}
        ${ACTIONENGINE_INCLUDE_DIR}
        ${ACTIONENGINE_THIRD_PARTY_INCLUDE_DIRS}
)
target_link_libraries(
        actData PRIVATE
        absl::base
        absl::log
        absl::log_internal_check_op
        absl::status
        absl::statusor
        absl::strings
        absl::strings_internal
        absl::time
        cppack
        act::msgpack
)

# ------------------------------------------------------------------

add_library(
        actDataPybind11 STATIC
)
add_library(
        act::data::pybind11 ALIAS
        actDataPybind11
)
target_sources(
        actDataPybind11 PRIVATE
        data_pybind11.cc
)
target_sources(
        actDataPybind11 PUBLIC
        data_pybind11.h
)
target_link_libraries(
        actDataPybind11 PRIVATE
        cppack
        absl::status
        absl::statusor
        act::data
        ${ACTIONENGINE_PYBIND11_DEPENDENCIES}
)
target_include_directories(
        actDataPybind11 PUBLIC
        ${ACTIONENGINE_PYBIND11_INCLUDE_DIRS}
)

install(
        TARGETS actData
        EXPORT actTargets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/act
        ARCHIVE DESTINATION lib/act
)