// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tools/diagnostics_log_parser.h"

#include <array>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <utility>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"

using std::array;
using std::cout;
using std::endl;
using std::ifstream;
using std::string;
using strings::Substitute;


namespace kudu {
namespace tools {

namespace {

Status ParseStackGroup(const rapidjson::Value& group_json,
                       StacksRecord::Group* group) {
  DCHECK(group);
  StacksRecord::Group ret;
  if (PREDICT_FALSE(!group_json.IsObject())) {
    return Status::InvalidArgument("expected stacks groups to be JSON objects");
  }
  if (!group_json.HasMember("tids") || !group_json.HasMember("stack")) {
    return Status::InvalidArgument("expected stacks groups to have frames and tids");
  }

  // Parse the tids.
  const auto& tids = group_json["tids"];
  if (PREDICT_FALSE(!tids.IsArray())) {
    return Status::InvalidArgument("expected 'tids' to be an array");
  }
  ret.tids.reserve(tids.Size());
  for (const auto* tid = tids.Begin();
       tid != tids.End();
       ++tid) {
    if (PREDICT_FALSE(!tid->IsNumber())) {
      return Status::InvalidArgument("expected 'tids' elements to be numeric");
    }
    ret.tids.push_back(tid->GetInt64());
  }

  // Parse and symbolize the stack trace itself.
  const auto& stack = group_json["stack"];
  if (PREDICT_FALSE(!stack.IsArray())) {
    return Status::InvalidArgument("expected 'stack' to be an array");
  }
  for (const auto* frame = stack.Begin();
       frame != stack.End();
       ++frame) {
    if (PREDICT_FALSE(!frame->IsString())) {
      return Status::InvalidArgument("expected 'stack' elements to be strings");
    }
    ret.frame_addrs.emplace_back(frame->GetString());
  }
  *group = std::move(ret);
  return Status::OK();
}

}  // anonymous namespace

const char* RecordTypeToString(RecordType r) {
  switch (r) {
    case RecordType::kStacks: return "stacks"; break;
    case RecordType::kSymbols: return "symbols"; break;
    case RecordType::kMetrics: return "metrics"; break;
    case RecordType::kUnknown: return "<unknown>"; break;
  }
  return "<unreachable>";
}

std::ostream& operator<<(std::ostream& o, RecordType r) {
  return o << RecordTypeToString(r);
}

Status SymbolsRecord::FromParsedLine(const ParsedLine& pl) {
  DCHECK_EQ(RecordType::kSymbols, pl.type());
  if (!pl.json()->IsObject()) {
    return Status::InvalidArgument("expected symbols data to be a JSON object");
  }
  for (auto it = pl.json()->MemberBegin();
       it != pl.json()->MemberEnd();
       ++it) {
    if (PREDICT_FALSE(!it->value.IsString())) {
      return Status::InvalidArgument("expected symbol values to be strings");
    }
    InsertIfNotPresent(&addr_to_symbol, it->name.GetString(), it->value.GetString());
  }
  return Status::OK();
}

Status MetricsRecord::FromParsedLine(const MetricsCollectingOpts& opts, const ParsedLine& pl) {
  DCHECK_EQ(RecordType::kMetrics, pl.type());
  if (!pl.json()->IsArray()) {
    return Status::InvalidArgument("Expected a metric json array");
  }

  MetricToEntities m;
  // Initialize the metric maps based on the specified options.
  for (const auto& metric_to_display_name : opts.simple_metric_names) {
    const auto& full_metric_name = metric_to_display_name.first;
    InsertIfNotPresent(&m, full_metric_name, EntityIdToValue());
  }
  for (const auto& metric_to_display_name : opts.rate_metric_names) {
    const auto& full_metric_name = metric_to_display_name.first;
    InsertIfNotPresent(&m, full_metric_name, EntityIdToValue());
  }
  for (const auto& metric_to_display_name : opts.hist_metric_names) {
    const auto& full_metric_name = metric_to_display_name.first;
    InsertIfNotPresent(&m, full_metric_name, EntityIdToValue());
  }

  // [{<entity>}, {<entity>}, {<entity>}]
  // Iterate through each entity blob and pick out the metrics.
  for (const auto* entity_json = pl.json()->Begin();
       entity_json != pl.json()->End();
       ++entity_json) {
    if (!entity_json->IsObject()) {
      return Status::InvalidArgument("Expected json object");
    }
    if (!entity_json->HasMember("type") ||
        !entity_json->HasMember("id") ||
        !entity_json->HasMember("metrics")) {
      return Status::InvalidArgument(
          Substitute("Incomplete entity entry: $0", string(entity_json->GetString())));
    }
    const string entity_type = (*entity_json)["type"].GetString();
    const string entity_id = (*entity_json)["id"].GetString();
    if (entity_type != "tablet" && entity_type != "server") {
      return Status::InvalidArgument(Substitute("Unexpected entity type: $0", entity_type));
    }
    if (entity_type == "tablet" &&
        !opts.tablet_ids.empty() && !ContainsKey(opts.tablet_ids, entity_id)) {
      // If the tablet id doesn't match the user's filter, ignore it. If the
      // tablet list is empty, no tablet ids are ignored.
      continue;
    }

    // [{"name":"<metric_name>","value":"<metric_value>"}]
    const auto& metrics = (*entity_json)["metrics"];
    if (!metrics.IsArray()) {
      return Status::InvalidArgument(
          Substitute("Expected metric array: $0", metrics.GetString()));
    }
    for (const auto* metric_json = metrics.Begin();
         metric_json != metrics.End();
         ++metric_json) {
      if (!metric_json->HasMember("name")) {
        return Status::InvalidArgument(
            Substitute("Expected 'name' field in metric entry: $0", metric_json->GetString()));
      }
      const string& metric_name = (*metric_json)["name"].GetString();
      const string& full_metric_name = Substitute("$0.$1", entity_type, metric_name);
      EntityIdToValue* entity_id_to_value = FindOrNull(m, full_metric_name);
      if (!entity_id_to_value) {
        // We're looking at a metric that the user hasn't requested. Ignore
        // this entry.
        continue;
      }
      MetricValue v;
      RETURN_NOT_OK(v.FromJson(*metric_json));

      // We expect that in a given line, each entity reports a given metric
      // only once. In case this isn't true, we just update the value.
      EmplaceOrUpdate(entity_id_to_value, entity_id, std::move(v));
    }
  }
  metric_to_entities.swap(m);
  return Status::OK();
}

Status MetricValue::FromJson(const rapidjson::Value& metric_json) {
  DCHECK(MetricType::kUninitialized == type_);
  DCHECK(boost::none == counts_);
  DCHECK(boost::none == value_);
  if (metric_json.HasMember("counts")) {
    // Add the counts from histogram metrics.
    const rapidjson::Value& counts_json = metric_json["counts"];
    if (counts_json.IsArray()) {
      return Status::InvalidArgument("Expected 'counts' to be array type");
    }
    std::vector<int64_t> counts = {};
    for (int i = 0; i < counts_json.Size(); i++) {
      counts.push_back(counts_json.GetInt64());
    }
    AddTokenCounts(counts, 1, &(*counts_));
    type_ = MetricType::kHistogram;
  } else if (metric_json.HasMember("value")) {
    const rapidjson::Value& value_json = metric_json["value"];
    if (!value_json.IsInt64()) {
      return Status::InvalidArgument("Expected 'value' to be an int type");
    }
    // Add the value from plain metrics.
    value_ = value_json.GetInt64();
    type_ = MetricType::kPlain;
  } else {
    return Status::InvalidArgument(
        Substitute("Unexpected metric formatting: $0", metric_json.GetString()));
  }
  return Status::OK();
}
MetricValue::MetricValue()
  : type_(MetricType::kUninitialized),
    value_(boost::none),
    counts_(boost::none) {}

Status MetricValue::MergeMetric(const MetricValue& v) {
  // If this is an empty value, just copy over the state.
  if (type_ == MetricType::kUninitialized) {
    type_ = v.type();
    counts_ = v.counts_;
    value_ = v.value_;
    return Status::OK();
  }

  // Return an error if there is otherwise a type mismatch.
  if (type_ != v.type()) {
    return Status::InvalidArgument("metric type mismatch");
  }

  // Now actually do the merging.
  switch (type_) {
    case MetricType::kHistogram: {
      DCHECK(counts_ && v.counts_);
      MergeTokenCounts(&(*counts_), *v.counts_);
      break;
    }
    case MetricType::kPlain: {
      DCHECK(value_ && v.value_);
      *value_ += *v.value_;
      break;
    }
    default:
      LOG(DFATAL) << "Unable to merge metric";
  }
  return Status::OK();
}

MetricCollectingLogVisitor::MetricCollectingLogVisitor(MetricsCollectingOpts opts)
    : opts_(std::move(opts)) {
  // Create an initial entity-to-value map for every metric of interest.
  for (const auto& full_and_display : opts_.simple_metric_names) {
    const auto& full_name = full_and_display.first;
    LOG(INFO) << "collecting simple metric " << full_name;
    InsertIfNotPresent(&metric_to_entities_, full_name, EntityIdToValue());
  }
  for (const auto& full_and_display : opts_.rate_metric_names) {
    const auto& full_name = full_and_display.first;
    InsertIfNotPresent(&metric_to_entities_, full_name, EntityIdToValue());
  }
  for (const auto& full_and_display : opts_.hist_metric_names) {
    const auto& full_name = full_and_display.first;
    InsertIfNotPresent(&metric_to_entities_, full_name, EntityIdToValue());
  }
}

Status MetricCollectingLogVisitor::VisitMetricsRecord(const MetricsRecord& mr) {
  // Iterate through the user-requested metrics and display what we need to.
  for (const auto& full_and_display : opts_.simple_metric_names) {
    const auto& full_name = full_and_display.first;
    const auto& entities_to_values = FindOrDie(mr.metric_to_entities, full_name);
    int64_t sum = 0;
    for (const auto& e : entities_to_values) {
      sum += *e.second.value_;
    }
    LOG(INFO) << full_name << ": " << sum;
  }
  for (const auto& full_and_display : opts_.rate_metric_names) {
    const auto& full_name = full_and_display.first;
    LOG(INFO) << "full_name: " << full_name;
  }
  for (const auto& full_and_display : opts_.hist_metric_names) {
    const auto& full_name = full_and_display.first;
    LOG(INFO) << "full_name: " << full_name;
  }

  // Update the visitor's metrics.
  for (const auto& metric_and_enties_map : mr.metric_to_entities) {
    const string& metric_name = metric_and_enties_map.first;

    // Update the visitor's internal map with the metrics from the record.
    // Note: The metric record should only have parsed metrics that the visitor
    // has in its internal map, so we can FindOrDie here.
    auto& visitor_entities_map = FindOrDie(metric_to_entities_, metric_name);
    const auto& record_entities_map = metric_and_enties_map.second;
    for (const auto& record_entity_and_val : record_entities_map) {
      const string& entity_id = record_entity_and_val.first;
      MetricValue& val = LookupOrInsert(&visitor_entities_map, entity_id, MetricValue());
      RETURN_NOT_OK_PREPEND(val.MergeMetric(record_entity_and_val.second),
          Substitute("Failed to merge metric $0 for entity $1", metric_name, entity_id));
    }
  }
  return Status::OK();
}

Status MetricCollectingLogVisitor::ParseRecord(const ParsedLine& pl) {
  // Do something with the metric record.
  if (pl.type() == RecordType::kMetrics) {
    MetricsRecord mr;
    RETURN_NOT_OK(mr.FromParsedLine(opts_, pl));
    RETURN_NOT_OK(VisitMetricsRecord(mr));
  }
  return Status::OK();
}

Status StackDumpingLogVisitor::ParseRecord(const ParsedLine& pl) {
  switch (pl.type()) {
    case RecordType::kSymbols: {
      SymbolsRecord sr;
      RETURN_NOT_OK(sr.FromParsedLine(pl));
      VisitSymbolsRecord(sr);
      break;
    }
    case RecordType::kStacks: {
      StacksRecord sr;
      RETURN_NOT_OK(sr.FromParsedLine(pl));
      VisitStacksRecord(sr);
      break;
    }
    default:
      break;
  }
  return Status::OK();
}

void StackDumpingLogVisitor::VisitSymbolsRecord(const SymbolsRecord& sr) {
  InsertOrUpdateMany(&symbols_, sr.addr_to_symbol.begin(), sr.addr_to_symbol.end());
}

void StackDumpingLogVisitor::VisitStacksRecord(const StacksRecord& sr) {
  if (!first_) {
    cout << endl << endl;
  }
  first_ = false;
  cout << "Stacks at " << sr.date_time << " (" << sr.reason << "):" << endl;
  for (const auto& group : sr.groups) {
    cout << "  tids=["
          << JoinMapped(group.tids, [](int t) { return std::to_string(t); }, ",")
          << "]" << endl;
    for (const auto& addr : group.frame_addrs) {
      // NOTE: passing 'kUnknownSymbols' as the default instead of a "foo"
      // literal is important to avoid capturing a reference to a temporary.
      // See the FindWithDefault() docs for details.
      const auto& sym = FindWithDefault(symbols_, addr, kUnknownSymbol);
      cout << std::setw(20) << addr << " " << sym << endl;
    }
  }
}

Status ParsedLine::Parse() {
  RETURN_NOT_OK(ParseHeader());
  return ParseJson();
}

Status ParsedLine::ParseHeader() {
  // Lines have the following format:
  //
  //     I0220 17:38:09.950546 metrics 1519177089950546 <json blob>...
  //
  // Logs from before Kudu 1.7.0 have the following format:
  //
  //     metrics 1519177089950546 <json blob>...
  Status s = DoParseV2Header();
  if (!s.ok()) {
    RETURN_NOT_OK(DoParseV1Header());
  }
  return Status::OK();
}

Status ParsedLine::DoParseV1Header() {
  if (line_.empty()) {
    return Status::InvalidArgument("empty line");
  }
  array<StringPiece, 3> fields = strings::Split(
      line_, strings::delimiter::Limit(" ", 2));
  int64_t time_us;
  if (!safe_strto64(fields[1].data(), fields[1].size(), &time_us)) {
    return Status::InvalidArgument("invalid timestamp", fields[1]);
  }
  timestamp_ = time_us;
  if (fields[0] == "metrics") {
    type_ = RecordType::kMetrics;
  } else {
    type_ = RecordType::kUnknown;
  }
  json_.emplace(fields[2].ToString());
  return Status::OK();
}

Status ParsedLine::DoParseV2Header() {
  if (line_.empty() || line_[0] != 'I') {
    return Status::InvalidArgument("lines must start with 'I'");
  }

  array<StringPiece, 5> fields = strings::Split(
      line_, strings::delimiter::Limit(" ", 4));
  fields[0].remove_prefix(1); // Remove the 'I'.
  // Sanity check the microsecond timestamp.
  // Eventually, it should be used when processing metrics records.
  int64_t time_us;
  if (!safe_strto64(fields[3].data(), fields[3].size(), &time_us)) {
    return Status::InvalidArgument("invalid timestamp", fields[3]);
  }
  timestamp_ = time_us;
  date_ = fields[0];
  time_ = fields[1];
  if (fields[2] == "symbols") {
    type_ = RecordType::kSymbols;
  } else if (fields[2] == "stacks") {
    type_ = RecordType::kStacks;
  } else if (fields[2] == "metrics") {
    type_ = RecordType::kMetrics;
  } else {
    type_ = RecordType::kUnknown;
  }
  json_.emplace(fields[4].ToString());
  return Status::OK();
}

Status ParsedLine::ParseJson() {
  // TODO(todd) JsonReader should be able to parse from a StringPiece
  // directly instead of making the copy here.
  Status s = json_->Init();
  if (!s.ok()) {
    json_ = boost::none;
    return s.CloneAndPrepend("invalid JSON payload");
  }
  return Status::OK();
}

string ParsedLine::date_time() const {
  return Substitute("$0 $1", date_, time_);
}

Status StacksRecord::FromParsedLine(const ParsedLine& pl) {
  date_time = pl.date_time();

  const rapidjson::Value& json = *pl.json();
  if (!json.IsObject()) {
    return Status::InvalidArgument("expected stacks data to be a JSON object");
  }

  // Parse reason if present. If not, we'll just leave it empty.
  if (json.HasMember("reason")) {
    if (!json["reason"].IsString()) {
      return Status::InvalidArgument("expected stacks 'reason' to be a string");
    }
    reason = json["reason"].GetString();
  }

  // Parse groups.
  if (PREDICT_FALSE(!json.HasMember("groups"))) {
    return Status::InvalidArgument("no 'groups' field in stacks object");
  }
  const auto& groups_json = json["groups"];
  if (!groups_json.IsArray()) {
    return Status::InvalidArgument("'groups' field should be an array");
  }

  for (const rapidjson::Value* group = groups_json.Begin();
       group != groups_json.End();
       ++group) {
    StacksRecord::Group g;
    RETURN_NOT_OK(ParseStackGroup(*group, &g));
    groups.emplace_back(std::move(g));
  }
  return Status::OK();
}

Status LogFileParser::Init() {
  errno = 0;
  if (!fstream_.is_open() || !HasNext()) {
    return Status::IOError(ErrnoToString(errno));
  }
  // XXX(awong): why?
  // Determine the parser type?
  // string first_line = PeekNextLine();
  return Status::OK();
}

bool LogFileParser::HasNext() {
  return fstream_.peek() != EOF;
}

string LogFileParser::PeekNextLine() {
  DCHECK(HasNext());
  string line;
  int initial_pos = fstream_.tellg();
  std::getline(fstream_, line);
  fstream_.seekg(initial_pos);
  return line;
}

Status LogFileParser::ParseLine() {
  DCHECK(HasNext());
  line_number_++;
  string line;
  std::getline(fstream_, line);
  ParsedLine pl(std::move(line));
  const string error_msg = Substitute("Failed to parse lin $0 in file $1",
                                      line_number_, path_);
  RETURN_NOT_OK_PREPEND(pl.Parse(), error_msg);
  RETURN_NOT_OK_PREPEND(log_visitor_->ParseRecord(pl), error_msg);
  return Status::OK();
}

Status LogFileParser::Parse() {
  string line;
  Status s;
  while (HasNext()) {
    s = ParseLine();
    if (s.IsEndOfFile()) {
      LOG(INFO) << "Reached end of time range";
      return Status::OK();
    }
    RETURN_NOT_OK(s);
  }
  return Status::OK();
}

} // namespace tools
} // namespace kudu

