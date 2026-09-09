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

#include "velox/experimental/cudf/connectors/hive/delta/CudfDeltaConnector.h"
#include "velox/experimental/cudf/tests/utils/CudfHiveConnectorTestBase.h"

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/delta/HiveDeltaSplit.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::cudf_velox::exec::test {

namespace {

constexpr char kCudfDeltaConnectorId[] = "test-cudf-delta";

namespace cudf_delta = ::facebook::velox::cudf_velox::connector::hive::delta;
namespace velox_connector = ::facebook::velox::connector;
namespace velox_delta = ::facebook::velox::connector::hive::delta;
namespace velox_hive = ::facebook::velox::connector::hive;

using facebook::velox::exec::test::AssertQueryBuilder;
using facebook::velox::exec::test::PlanBuilder;
using velox_delta::HiveDeltaSplit;
using velox_hive::HiveColumnHandle;

class CudfDeltaReadTest : public CudfHiveConnectorTestBase {
 protected:
  void SetUp() override {
    CudfHiveConnectorTestBase::SetUp();

    cudf_delta::CudfDeltaConnectorFactory factory;
    auto deltaConnector = factory.newConnector(
        kCudfDeltaConnectorId,
        std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>{}),
        ioExecutor_.get());
    velox_connector::ConnectorRegistry::global().insert(
        deltaConnector->connectorId(), deltaConnector);
  }

  void TearDown() override {
    velox_connector::ConnectorRegistry::global().erase(kCudfDeltaConnectorId);
    CudfHiveConnectorTestBase::TearDown();
  }

  std::vector<std::shared_ptr<velox_connector::ConnectorSplit>> makeDeltaSplits(
      const std::string& dataFilePath,
      const std::unordered_map<std::string, std::optional<std::string>>&
          partitionKeys = {},
      const std::unordered_map<std::string, std::string>& infoColumns = {}) {
    const auto fileSize = filesystems::getFileSystem(dataFilePath, nullptr)
                              ->openFileForRead(dataFilePath)
                              ->size();
    return {std::make_shared<HiveDeltaSplit>(
        kCudfDeltaConnectorId,
        dataFilePath,
        dwio::common::FileFormat::PARQUET,
        0,
        fileSize,
        partitionKeys,
        std::nullopt,
        std::unordered_map<std::string, std::string>{
            {"table_format", "hive-delta"}},
        nullptr,
        /*cacheable=*/true,
        infoColumns)};
  }

  static std::shared_ptr<HiveColumnHandle> makeHandle(
      const std::string& name,
      const TypePtr& type,
      HiveColumnHandle::ColumnType columnType =
          HiveColumnHandle::ColumnType::kRegular) {
    return std::make_shared<HiveColumnHandle>(
        name, columnType, type, type, std::vector<common::Subfield>{});
  }
};

TEST_F(CudfDeltaReadTest, partitionInfoAndMissingColumns) {
  auto data = makeRowVector({"id", "value"},
                            {
                                makeFlatVector<int64_t>({1, 2, 3}),
                                makeFlatVector<int32_t>({10, 20, 30}),
                            });
  auto dataFile = TempFilePath::create();
  writeToFile(dataFile->getPath(), data);

  auto detailType = ROW({"note", "rank"}, {VARCHAR(), INTEGER()});
  auto tableType =
      ROW({"id", "region", "added", "detail", "$path", "value"},
          {BIGINT(),
           VARCHAR(),
           INTEGER(),
           detailType,
           VARCHAR(),
           INTEGER()});
  velox_connector::ColumnHandleMap assignments;
  assignments["id"] = makeHandle("id", BIGINT());
  assignments["region"] = makeHandle(
      "region", VARCHAR(), HiveColumnHandle::ColumnType::kPartitionKey);
  assignments["added"] = makeHandle("added", INTEGER());
  assignments["detail"] = makeHandle("detail", detailType);
  assignments["$path"] = makeHandle(
      "$path", VARCHAR(), HiveColumnHandle::ColumnType::kSynthesized);
  assignments["value"] = makeHandle("value", INTEGER());

  auto plan = PlanBuilder()
                  .startTableScan()
                  .connectorId(kCudfDeltaConnectorId)
                  .outputType(tableType)
                  .dataColumns(tableType)
                  .assignments(assignments)
                  .endTableScan()
                  .planNode();
  auto expected =
      makeRowVector(tableType->names(),
                    {
                        data->childAt(0),
                        makeFlatVector<std::string>({"US", "US", "US"}),
                        makeNullConstant(TypeKind::INTEGER, 3),
                        BaseVector::createNullConstant(detailType, 3, pool()),
                        makeFlatVector<std::string>(
                            {"delta-file", "delta-file", "delta-file"}),
                        data->childAt(1),
                    });

  AssertQueryBuilder(plan)
      .splits(makeDeltaSplits(
          dataFile->getPath(), {{"region", "US"}}, {{"$path", "delta-file"}}))
      .assertResults({expected});
}

TEST_F(CudfDeltaReadTest, injectedOnlyProjectionAndFilter) {
  auto data = makeRowVector({"id"},
                            {
                                makeFlatVector<int64_t>({1, 2, 3}),
                            });
  auto dataFile = TempFilePath::create();
  writeToFile(dataFile->getPath(), data);

  auto dataColumns = ROW({"id", "region"}, {BIGINT(), VARCHAR()});
  auto outputType = ROW({"region"}, {VARCHAR()});
  velox_connector::ColumnHandleMap assignments;
  assignments["region"] = makeHandle(
      "region", VARCHAR(), HiveColumnHandle::ColumnType::kPartitionKey);
  const auto makePlan = [&](const std::string& filter) {
    return PlanBuilder()
        .startTableScan()
        .connectorId(kCudfDeltaConnectorId)
        .outputType(outputType)
        .dataColumns(dataColumns)
        .assignments(assignments)
        .subfieldFilter(filter)
        .endTableScan()
        .planNode();
  };
  auto expected = makeRowVector(
      {"region"}, {makeFlatVector<std::string>({"US", "US", "US"})});
  AssertQueryBuilder(makePlan("region = 'US'"))
      .splits(makeDeltaSplits(dataFile->getPath(), {{"region", "US"}}))
      .assertResults({expected});

  auto empty = makeRowVector(
      {"region"}, {makeFlatVector<std::string>(std::vector<std::string>{})});
  AssertQueryBuilder(makePlan("region = 'CA'"))
      .splits(makeDeltaSplits(dataFile->getPath(), {{"region", "US"}}))
      .assertResults({empty});

  auto countPlan = PlanBuilder()
                       .startTableScan()
                       .connectorId(kCudfDeltaConnectorId)
                       .outputType(ROW({}, {}))
                       .dataColumns(dataColumns)
                       .assignments({})
                       .endTableScan()
                       .singleAggregation({}, {"count(1)"})
                       .planNode();
  auto expectedCount =
      makeRowVector({"a0"}, {makeFlatVector<int64_t>({3})});
  AssertQueryBuilder(countPlan)
      .splits(makeDeltaSplits(dataFile->getPath()))
      .assertResults({expectedCount});
}

TEST_F(CudfDeltaReadTest, deltaPartitionEncoding) {
  auto data = makeRowVector({"id"},
                            {
                                makeFlatVector<int64_t>({1, 2}),
                            });
  auto dataFile = TempFilePath::create();
  writeToFile(dataFile->getPath(), data);

  auto tableType = ROW({"id", "partition_date", "empty", "null_value"},
                       {BIGINT(), DATE(), VARCHAR(), VARCHAR()});
  velox_connector::ColumnHandleMap assignments;
  assignments["id"] = makeHandle("id", BIGINT());
  for (const auto& name : {"partition_date", "empty", "null_value"}) {
    assignments[name] = makeHandle(name,
                                   tableType->findChild(name),
                                   HiveColumnHandle::ColumnType::kPartitionKey);
  }
  auto plan = PlanBuilder()
                  .startTableScan()
                  .connectorId(kCudfDeltaConnectorId)
                  .outputType(tableType)
                  .dataColumns(tableType)
                  .assignments(assignments)
                  .endTableScan()
                  .planNode();
  const auto days = DATE()->toDays("2025-06-05");
  auto expected =
      makeRowVector(tableType->names(),
                    {
                        data->childAt(0),
                        makeFlatVector<int32_t>({days, days}, DATE()),
                        makeFlatVector<std::string>({"", ""}),
                        makeNullConstant(TypeKind::VARCHAR, 2),
                    });

  AssertQueryBuilder(plan)
      .splits(makeDeltaSplits(dataFile->getPath(),
                              {{"partition_date", "2025-06-05"},
                               {"empty", ""},
                               {"null_value", std::nullopt}}))
      .assertResults({expected});
}

} // namespace
} // namespace facebook::velox::cudf_velox::exec::test
