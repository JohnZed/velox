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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/delta/HiveDeltaSplit.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/tz/TimeZoneMap.h"

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
      const std::unordered_map<std::string, std::string>& infoColumns = {},
      bool hasDeletionVector = false,
      velox_delta::DeltaColumnMappingMode columnMappingMode =
          velox_delta::DeltaColumnMappingMode::kNone) {
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
        infoColumns,
        /*fileProperties=*/std::nullopt,
        hasDeletionVector,
        columnMappingMode)};
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

TEST_F(CudfDeltaReadTest, rejectsDeletionVectors) {
  auto data = makeRowVector({"id"}, {makeFlatVector<int64_t>({1, 2, 3})});
  auto dataFile = TempFilePath::create();
  writeToFile(dataFile->getPath(), data);

  const auto tableType = ROW({"id", "region"}, {BIGINT(), VARCHAR()});
  // Cover physical reads, synthesized-only reads, and zero-column counts.
  for (const auto& outputType :
       {ROW({"id"}, {BIGINT()}), ROW({"region"}, {VARCHAR()}), ROW({}, {})}) {
    SCOPED_TRACE(outputType->toString());
    velox_connector::ColumnHandleMap assignments;
    for (const auto& name : outputType->names()) {
      assignments[name] = makeHandle(
          name,
          tableType->findChild(name),
          name == "region" ? HiveColumnHandle::ColumnType::kPartitionKey
                           : HiveColumnHandle::ColumnType::kRegular);
    }
    auto plan = PlanBuilder()
                    .startTableScan()
                    .connectorId(kCudfDeltaConnectorId)
                    .outputType(outputType)
                    .dataColumns(tableType)
                    .assignments(assignments)
                    .endTableScan()
                    .singleAggregation({}, {"count(1)"})
                    .planNode();
    auto expectedCount = makeRowVector({"a0"}, {makeFlatVector<int64_t>({3})});
    AssertQueryBuilder(plan)
        .splits(makeDeltaSplits(dataFile->getPath(), {{"region", "US"}}))
        .assertResults({expectedCount});

    VELOX_ASSERT_USER_THROW(
        AssertQueryBuilder(plan)
            .splits(makeDeltaSplits(dataFile->getPath(),
                                    {{"region", "US"}},
                                    {},
                                    /*hasDeletionVector=*/true))
            .copyResults(pool()),
        "Reading Delta files with a deletion vector is not supported.");
  }
}

TEST_F(CudfDeltaReadTest, decimalProjectionAcrossTwoSplits) {
  const auto decimalType = DECIMAL(10, 2);
  auto data = makeRowVector(
      {"id", "amount"},
      {makeFlatVector<int64_t>({1, 2}),
       makeFlatVector<int64_t>({12345, -6789}, decimalType)});
  auto dataFile = TempFilePath::create();
  writeToFile(dataFile->getPath(), data);
  const auto tableType = ROW(
      {"region", "id", "added", "amount"},
      {VARCHAR(), BIGINT(), INTEGER(), decimalType});
  velox_connector::ColumnHandleMap assignments;
  for (const auto& name : tableType->names()) {
    assignments[name] = makeHandle(
        name,
        tableType->findChild(name),
        name == "region" ? HiveColumnHandle::ColumnType::kPartitionKey
                         : HiveColumnHandle::ColumnType::kRegular);
  }
  auto plan = PlanBuilder()
                  .startTableScan()
                  .connectorId(kCudfDeltaConnectorId)
                  .outputType(tableType)
                  .dataColumns(tableType)
                  .assignments(assignments)
                  .endTableScan()
                  .planNode();
  auto splits = makeDeltaSplits(dataFile->getPath(), {{"region", "US"}});
  auto second = makeDeltaSplits(dataFile->getPath(), {{"region", "US"}});
  splits.insert(splits.end(), second.begin(), second.end());
  auto expected = makeRowVector(
      tableType->names(),
      {makeFlatVector<std::string>({"US", "US"}),
       data->childAt(0),
       makeNullConstant(TypeKind::INTEGER, 2),
       data->childAt(1)});
  AssertQueryBuilder(plan).maxDrivers(1).splits(splits).assertResults(
      {expected, expected});
}

TEST_F(CudfDeltaReadTest, columnMappingMissingPhysicalColumn) {
  auto data = makeRowVector({"id"}, {makeFlatVector<int64_t>({1, 2})});
  auto dataFile = TempFilePath::create();
  writeToFile(dataFile->getPath(), data);
  const auto tableType = ROW({"id", "missing"}, {BIGINT(), BIGINT()});
  velox_connector::ColumnHandleMap assignments;
  assignments["id"] = makeHandle("id", BIGINT());
  assignments["missing"] = makeHandle("missing", BIGINT());
  auto plan = PlanBuilder()
                  .startTableScan()
                  .connectorId(kCudfDeltaConnectorId)
                  .outputType(tableType)
                  .dataColumns(tableType)
                  .assignments(assignments)
                  .endTableScan()
                  .planNode();
  auto expected = makeRowVector(
      tableType->names(),
      {data->childAt(0), makeNullConstant(TypeKind::BIGINT, 2)});
  for (auto mode : {velox_delta::DeltaColumnMappingMode::kNone,
                    velox_delta::DeltaColumnMappingMode::kName}) {
    AssertQueryBuilder(plan)
        .splits(makeDeltaSplits(dataFile->getPath(), {}, {}, false, mode))
        .assertResults({expected});
  }
  VELOX_ASSERT_USER_THROW(
      AssertQueryBuilder(plan)
          .splits(makeDeltaSplits(
              dataFile->getPath(), {}, {}, false,
              velox_delta::DeltaColumnMappingMode::kId))
          .copyResults(pool()),
      "field-id resolution is not supported");
}

TEST_F(CudfDeltaReadTest, gpuScanCapability) {
  velox_connector::ColumnHandleMap assignments;
  assignments["id"] = makeHandle("id", BIGINT());
  assignments["items"] =
      makeHandle("items", ARRAY(ROW({"x"}, {BIGINT()})));
  EXPECT_TRUE(cudf_delta::isCudfDeltaScanSupported(assignments));

  assignments["attributes"] =
      makeHandle("attributes", MAP(VARCHAR(), VARCHAR()));
  EXPECT_FALSE(cudf_delta::isCudfDeltaScanSupported(assignments));

  assignments.clear();
  std::vector<common::Subfield> requiredSubfields;
  requiredSubfields.emplace_back("root.child");
  assignments["nested"] = std::make_shared<HiveColumnHandle>(
      "nested",
      HiveColumnHandle::ColumnType::kRegular,
      BIGINT(),
      BIGINT(),
      std::move(requiredSubfields));
  EXPECT_FALSE(cudf_delta::isCudfDeltaScanSupported(assignments));
}

// Presto's Delta connector leaves partition columns out of dataColumns and
// describes them only through filter column handles. Filters on such columns,
// alone or combined with data column filters, must resolve their types from
// the handles. Filters are built directly, as Presto sends them, because
// PlanBuilder parses filter strings against dataColumns only.
TEST_F(CudfDeltaReadTest, partitionColumnsOutsideDataColumns) {
  auto data = makeRowVector({"id"}, {makeFlatVector<int64_t>({1, 2, 3})});
  auto dataFile = TempFilePath::create();
  writeToFile(dataFile->getPath(), data);

  auto dataColumns = ROW({"id"}, {BIGINT()});
  auto idHandle = makeHandle("id", BIGINT());
  auto regionHandle = makeHandle(
      "region", VARCHAR(), HiveColumnHandle::ColumnType::kPartitionKey);

  // Filter-only partition column.
  const auto makeFilterOnlyPlan = [&](std::unique_ptr<common::Filter> filter) {
    common::SubfieldFilters filters;
    filters[common::Subfield("region")] = std::move(filter);
    return PlanBuilder()
        .startTableScan()
        .connectorId(kCudfDeltaConnectorId)
        .outputType(ROW({"id"}, {BIGINT()}))
        .dataColumns(dataColumns)
        .assignments({{"id", idHandle}})
        .filterColumnHandles({idHandle, regionHandle})
        .subfieldFiltersMap(filters)
        .endTableScan()
        .planNode();
  };
  auto allRows = makeRowVector({"id"}, {makeFlatVector<int64_t>({1, 2, 3})});
  auto noRows =
      makeRowVector({"id"}, {makeFlatVector<int64_t>(std::vector<int64_t>{})});
  AssertQueryBuilder(makeFilterOnlyPlan(velox::exec::equal("US")))
      .splits(makeDeltaSplits(dataFile->getPath(), {{"region", "US"}}))
      .assertResults({allRows});
  AssertQueryBuilder(makeFilterOnlyPlan(velox::exec::equal("CA")))
      .splits(makeDeltaSplits(dataFile->getPath(), {{"region", "US"}}))
      .assertResults({noRows});
  AssertQueryBuilder(makeFilterOnlyPlan(velox::exec::isNull()))
      .splits(makeDeltaSplits(dataFile->getPath(), {{"region", std::nullopt}}))
      .assertResults({allRows});
  AssertQueryBuilder(makeFilterOnlyPlan(velox::exec::isNotNull()))
      .splits(makeDeltaSplits(dataFile->getPath(), {{"region", std::nullopt}}))
      .assertResults({noRows});

  // Projected partition column filtered together with a data column.
  common::SubfieldFilters combinedFilters;
  combinedFilters[common::Subfield("region")] = velox::exec::equal("US");
  combinedFilters[common::Subfield("id")] = velox::exec::greaterThan(1);
  auto combinedPlan =
      PlanBuilder()
          .startTableScan()
          .connectorId(kCudfDeltaConnectorId)
          .outputType(ROW({"id", "region"}, {BIGINT(), VARCHAR()}))
          .dataColumns(dataColumns)
          .assignments({{"id", idHandle}, {"region", regionHandle}})
          .filterColumnHandles({idHandle, regionHandle})
          .subfieldFiltersMap(combinedFilters)
          .endTableScan()
          .planNode();
  auto expected = makeRowVector(
      {"id", "region"},
      {makeFlatVector<int64_t>({2, 3}),
       makeFlatVector<std::string>({"US", "US"})});
  AssertQueryBuilder(combinedPlan)
      .splits(makeDeltaSplits(dataFile->getPath(), {{"region", "US"}}))
      .assertResults({expected});
}

// cuDF has no zoned timestamp type; the reader packs UTC Parquet timestamps
// into Velox's TIMESTAMP WITH TIME ZONE representation.
TEST_F(CudfDeltaReadTest, timestampWithTimeZoneColumn) {
  const std::vector<int64_t> millis{0, 1'631'081'471'000, -86'400'000};
  auto data = makeRowVector(
      {"ts"},
      {makeNullableFlatVector<Timestamp>(
          {Timestamp::fromMillis(millis[0]),
           Timestamp::fromMillis(millis[1]),
           std::nullopt,
           Timestamp::fromMillis(millis[2])})});
  auto dataFile = TempFilePath::create();
  writeToFile(dataFile->getPath(), data);

  auto rowType = ROW({"ts"}, {TIMESTAMP_WITH_TIME_ZONE()});
  auto plan =
      PlanBuilder()
          .startTableScan()
          .connectorId(kCudfDeltaConnectorId)
          .outputType(rowType)
          .dataColumns(rowType)
          .assignments({{"ts", makeHandle("ts", TIMESTAMP_WITH_TIME_ZONE())}})
          .endTableScan()
          .planNode();
  const auto utc = tz::getTimeZoneID("UTC");
  auto expected = makeRowVector(
      {"ts"},
      {makeNullableFlatVector<int64_t>(
          {pack(millis[0], utc),
           pack(millis[1], utc),
           std::nullopt,
           pack(millis[2], utc)},
          TIMESTAMP_WITH_TIME_ZONE())});
  AssertQueryBuilder(plan)
      .splits(makeDeltaSplits(dataFile->getPath()))
      .assertResults({expected});
}

// With adjust_timestamp_to_session_timezone, timestamp partition values are
// wall clock times in the session zone, as in the CPU Delta reader.
TEST_F(CudfDeltaReadTest, timestampPartitionValueUsesSessionTimezone) {
  auto data = makeRowVector({"id"}, {makeFlatVector<int64_t>({1})});
  auto dataFile = TempFilePath::create();
  writeToFile(dataFile->getPath(), data);

  auto plan = PlanBuilder()
                  .startTableScan()
                  .connectorId(kCudfDeltaConnectorId)
                  .outputType(ROW({"id", "ts"}, {BIGINT(), TIMESTAMP()}))
                  .dataColumns(ROW({"id"}, {BIGINT()}))
                  .assignments(
                      {{"id", makeHandle("id", BIGINT())},
                       {"ts",
                        makeHandle(
                            "ts",
                            TIMESTAMP(),
                            HiveColumnHandle::ColumnType::kPartitionKey)}})
                  .endTableScan()
                  .planNode();
  // 2021-09-08 11:11:11 in America/Los_Angeles (PDT, UTC-7).
  auto expected = makeRowVector(
      {"id", "ts"},
      {makeFlatVector<int64_t>({1}),
       makeFlatVector<Timestamp>({Timestamp(1'631'124'671, 0)})});
  AssertQueryBuilder(plan)
      .config(core::QueryConfig::kSessionTimezone, "America/Los_Angeles")
      .config(core::QueryConfig::kAdjustTimestampToTimezone, "true")
      .splits(
          makeDeltaSplits(dataFile->getPath(), {{"ts", "2021-09-08 11:11:11"}}))
      .assertResults({expected});
}

} // namespace
} // namespace facebook::velox::cudf_velox::exec::test
