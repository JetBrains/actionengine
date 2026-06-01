// Copyright 2026 The Action Engine Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "actionengine/util/telemetry.h"

#include <opentelemetry/exporters/ostream/span_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/batch_span_processor_runtime_options.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/provider.h>
#include <opentelemetry/sdk/trace/random_id_generator.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/tracer_provider.h>

namespace act::telemetry {

namespace trace_exporter = opentelemetry::exporter::trace;
namespace trace_sdk = opentelemetry::sdk::trace;

static opentelemetry::sdk::trace::IdGenerator& GetIdGenerator() {
  thread_local opentelemetry::sdk::trace::RandomIdGenerator id_generator;
  return id_generator;
}

static void InitTracer() {
  auto exporter = trace_exporter::OStreamSpanExporterFactory::Create();
  auto processor =
      trace_sdk::SimpleSpanProcessorFactory::Create(std::move(exporter));

  std::unique_ptr sdk_provider =
      trace_sdk::TracerProviderFactory::Create(std::move(processor));

  // Set the global trace provider
  const std::shared_ptr<opentelemetry::trace::TracerProvider>& api_provider =
      std::move(sdk_provider);
  trace_sdk::Provider::SetTracerProvider(api_provider);
}

void CleanupTracer() {
  const std::shared_ptr<opentelemetry::trace::TracerProvider> noop;
  trace_sdk::Provider::SetTracerProvider(noop);
}

std::string GetTelemetryHeaderName(std::string_view name) {
  return absl::StrCat(kActionEngineHeaderPrefix, "otel-", name);
}

opentelemetry::trace::SpanId GenerateSpanId() {
  return GetIdGenerator().GenerateSpanId();
}

opentelemetry::trace::TraceId GenerateTraceId() {
  return GetIdGenerator().GenerateTraceId();
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>
GetHttpTracerProvider(std::string_view url) {
  trace_sdk::BatchSpanProcessorOptions batch_processor_opts{};
  opentelemetry::exporter::otlp::OtlpHttpExporterOptions opts;
  opts.url = url;
  std::string auth = absl::StrCat(
      "Basic ", absl::Base64Escape(absl::StrCat(
                    "pk-lf-cc1a3156-b414-4fd3-a74a-63947d9c0c4c", ":",
                    "sk-lf-a256e422-f842-4933-abe5-a390f48ac9e0")));
  opts.http_headers.emplace(
      std::pair{std::string("Authorization"), std::move(auth)});
  auto exporter =
      opentelemetry::exporter::otlp::OtlpHttpExporterFactory::Create(opts);
  auto processor = trace_sdk::BatchSpanProcessorFactory::Create(
      std::move(exporter), batch_processor_opts);
  std::shared_ptr<trace_api::TracerProvider> provider =
      trace_sdk::TracerProviderFactory::Create(std::move(processor));
  return provider;
}

void SetGlobalTracerProvider() {
  SetGlobalTracerProvider(
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
          nullptr));
}

void SetGlobalTracerProvider(
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>
        provider) {
  trace_sdk::Provider::SetTracerProvider(std::move(provider));
}

void InitDefaultTracerProvider() {
  InitTracer();
}

std::string GenerateSpanIdAsStdString() {
  return *ConvertTo<std::string>(GenerateSpanId());
}

std::string GenerateTraceIdAsStdString() {
  return *ConvertTo<std::string>(GenerateTraceId());
}

static opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>
GetTracer() {
  const auto provider = trace_api::Provider::GetTracerProvider();
  return provider->GetTracer(opentelemetry::nostd::string_view("actionengine"));
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> CreateSpan(
    std::string_view name) {
  return CreateSpan(
      name,
      opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>(nullptr));
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> CreateSpan(
    std::string_view name, opentelemetry::trace::SpanContext parent_context) {
  opentelemetry::trace::StartSpanOptions opts;
  opts.kind = opentelemetry::trace::SpanKind::kInternal;
  opts.parent = parent_context;
  return CreateSpan(name, std::move(opts));
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> CreateSpan(
    std::string_view name,
    const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>&
        parent) {
  opentelemetry::trace::StartSpanOptions opts;
  opts.kind = opentelemetry::trace::SpanKind::kInternal;
  if (parent != nullptr) {
    opts.parent = parent->GetContext();
  } else {
    opentelemetry::context::Context context_root{
        opentelemetry::trace::kIsRootSpanKey, true};
    opts.parent = context_root;
  }
  return CreateSpan(name, std::move(opts));
}

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> CreateSpan(
    std::string_view name, opentelemetry::trace::StartSpanOptions options) {
  return GetTracer()->StartSpan({name.data(), name.size()}, std::move(options));
}

}  // namespace act::telemetry

absl::Status opentelemetry::trace::EgltAssignInto(
    SpanId from, std::string* absl_nonnull to) {
  CHECK(to != nullptr);
  static_assert(sizeof(std::string::value_type) == sizeof(uint8_t));
  *to = std::string{
      reinterpret_cast<const std::string::value_type*>(from.Id().data()),
      from.Id().size()};
  return absl::OkStatus();
}

absl::Status opentelemetry::trace::EgltAssignInto(std::string from,
                                                  SpanId* to) {
  CHECK(to != nullptr);
  static_assert(sizeof(std::string::value_type) == sizeof(uint8_t));
  if (from.size() != opentelemetry::trace::SpanId::kSize) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid span ID length: ", from.size()));
  }
  opentelemetry::nostd::span<uint8_t, opentelemetry::trace::SpanId::kSize> id(
      reinterpret_cast<uint8_t*>(from.data()), from.size());
  *to = SpanId(id);
  return absl::OkStatus();
}

absl::Status opentelemetry::trace::EgltAssignInto(TraceId from,
                                                  std::string* to) {
  CHECK(to != nullptr);
  static_assert(sizeof(std::string::value_type) == sizeof(uint8_t));
  *to = std::string{
      reinterpret_cast<const std::string::value_type*>(from.Id().data()),
      from.Id().size()};
  return absl::OkStatus();
}

absl::Status opentelemetry::trace::EgltAssignInto(std::string from,
                                                  TraceId* to) {
  CHECK(to != nullptr);
  static_assert(sizeof(std::string::value_type) == sizeof(uint8_t));
  if (from.size() != opentelemetry::trace::TraceId::kSize) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid trace ID length: ", from.size()));
  }
  opentelemetry::nostd::span<uint8_t, opentelemetry::trace::TraceId::kSize> id(
      reinterpret_cast<uint8_t*>(from.data()), from.size());
  *to = TraceId(id);
  return absl::OkStatus();
}