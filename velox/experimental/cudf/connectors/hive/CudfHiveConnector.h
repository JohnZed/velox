/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/experimental/cudf/connectors/hive/CudfHiveConfig.h"

#include "velox/connectors/hive/HiveConnector.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/types.hpp>

#include <optional>
#include <string>

namespace facebook::velox::cudf_velox::connector::hive {

using namespace facebook::velox::connector;
using namespace facebook::velox::config;

/// Returns the name of the first column of a plain Hive table scan that the
/// cuDF Hive reader cannot produce, or std::nullopt when the scan is fully
/// supported.
///
/// The plain Hive cuDF reader only reads columns stored in the Parquet file;
/// unlike the Delta and Iceberg cuDF readers it does not synthesize partition
/// or info column values. Unsupported columns are:
///  - Output columns whose handle is not a regular (file) column, e.g.
///    partition keys or synthesized columns such as $path.
///  - Columns referenced by the table handle's subfield or remaining filter
///    that are neither data columns nor regular output columns. Presto's Hive
///    connector sends no handles for filter-only columns, so a filter-only
///    partition column is only recognizable as a name missing from
///    dataColumns. The Hive coordinator usually enforces partition filters by
///    pruning, but e.g. `part = 'a' OR x > 5` reaches the worker.
/// The cuDF table scan adapter uses this to keep such scans on the CPU.
std::optional<std::string> findUnsupportedCudfHiveScanColumn(
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap& assignments);

class CudfHiveConnector final
    : public ::facebook::velox::connector::hive::HiveConnector {
 public:
  CudfHiveConnector(
      const std::string& id,
      std::shared_ptr<const ConfigBase> config,
      folly::Executor* executor);

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const ColumnHandleMap& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override final;

  bool canAddDynamicFilter() const override {
    return false;
  }

  bool supportsSplitPreload() const override {
    return true;
  }

  // TODO (dm): Re-add data sink

 protected:
  // TODO (dm): rename parquetconfig
  const std::shared_ptr<CudfHiveConfig> cudfHiveConfig_;
};

class CudfHiveConnectorFactory
    : public ::facebook::velox::connector::hive::HiveConnectorFactory {
 public:
  CudfHiveConnectorFactory()
      : ::facebook::velox::connector::hive::HiveConnectorFactory() {}

  explicit CudfHiveConnectorFactory(const char* connectorName)
      : ::facebook::velox::connector::hive::HiveConnectorFactory(
            connectorName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override;
};

} // namespace facebook::velox::cudf_velox::connector::hive
