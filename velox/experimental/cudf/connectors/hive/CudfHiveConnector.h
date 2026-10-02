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
#include "velox/experimental/cudf/connectors/hive/CudfTableScanSupport.h"

#include "velox/connectors/hive/HiveConnector.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/types.hpp>

#include <optional>
#include <string>

namespace facebook::velox::cudf_velox::connector::hive {

using namespace facebook::velox::connector;
using namespace facebook::velox::config;

class CudfHiveConnector final
    : public ::facebook::velox::connector::hive::HiveConnector,
      public CudfTableScanSupport {
 public:
  CudfHiveConnector(
      const std::string& id,
      std::shared_ptr<const ConfigBase> config,
      folly::Executor* executor);

  /// The plain Hive cuDF reader only reads columns stored in the Parquet
  /// file; unlike the Delta and Iceberg cuDF readers it does not synthesize
  /// partition or info column values. Unsupported scans have:
  ///  - An output column whose handle is not a regular (file) column, e.g. a
  ///    partition key or a synthesized column such as $path.
  ///  - A subfield or remaining filter on a column that is neither a data
  ///    column nor a regular output column. Presto's Hive connector sends no
  ///    handles for filter-only columns, so a filter-only partition column is
  ///    only recognizable as a name missing from dataColumns. The coordinator
  ///    usually enforces partition filters by pruning, but e.g.
  ///    `part = 'a' OR x > 5` reaches the worker.
  std::optional<std::string> unsupportedGpuScanReason(
      const ConnectorTableHandlePtr& tableHandle,
      const ColumnHandleMap& assignments) const override;

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
