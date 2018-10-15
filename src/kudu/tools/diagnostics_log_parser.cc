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
#include <set>
#include <string>
#include <utility>
#include <vector>

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
using std::multiset;
using std::string;
using std::vector;
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

  // Each json blob has a list of metrics for entities:
  //   [{<entity>}, {<entity>}, {<entity>}]
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

    // Each entity has a list of metrics:
    //   [{"name":"<metric_name>","value":"<metric_value>"},
    //    {"name":"<hist_metric_name>","counts":"<hist_metric_counts>"]
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
      Status s = v.FromJson(*metric_json);
      if (!s.ok()) {
        continue;
      }
      // We expect that in a given line, each entity reports a given metric
      // only once. In case this isn't true, we just update the value.
      EmplaceOrUpdate(entity_id_to_value, entity_id, std::move(v));
    }
  }
  metric_to_entities.swap(m);
  timestamp = pl.timestamp();
  return Status::OK();
}

Status MetricValue::FromJson(const rapidjson::Value& metric_json) {
  DCHECK(MetricType::kUninitialized == type_);
  DCHECK(boost::none == counts_);
  DCHECK(boost::none == value_);
  if (metric_json.HasMember("counts") && metric_json.HasMember("values")) {
    // Add the counts from histogram metrics.
    const rapidjson::Value& counts_json = metric_json["counts"];
    const rapidjson::Value& values_json = metric_json["values"];
    if (!counts_json.IsArray() || !values_json.IsArray()) {
      return Status::InvalidArgument(Substitute("Expected 'counts' and 'values' to be arrays: $0",
                                                metric_json.GetString()));
    }
    if (counts_json.Size() != values_json.Size()) {
      return Status::InvalidArgument("Expected 'counts' and 'values' to be the same size");
    }
    std::map<int64_t, int> counts;
    for (int i = 0; i < counts_json.Size(); i++) {
      int64_t v = values_json[i].GetInt64();
      int c = counts_json[i].GetInt();
      InsertOrUpdate(&counts, v, c);
    }
    counts_ = std::move(counts);
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

Status MetricValue::MergeMetric(MetricValue v) {
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
      counts_->swap(*v.counts_);
      break;
    }
    case MetricType::kPlain: {
      DCHECK(value_ && v.value_);
      *value_ = *v.value_;
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
    LOG(INFO) << "collecting rate metric " << full_name;
    InsertIfNotPresent(&metric_to_entities_, full_name, EntityIdToValue());
  }
  for (const auto& full_and_display : opts_.hist_metric_names) {
    const auto& full_name = full_and_display.first;
    LOG(INFO) << "collecting hist metric " << full_name;
    InsertIfNotPresent(&metric_to_entities_, full_name, EntityIdToValue());
  }
}

Status MetricCollectingLogVisitor::VisitMetricsRecord(const MetricsRecord& mr) {
  // Iterate through the user-requested metrics and display what we need to.
  cout << mr.timestamp;
  for (const auto& full_and_display : opts_.simple_metric_names) {
    const auto& full_name = full_and_display.first;
    const auto& mr_entities_to_vals = FindOrDie(mr.metric_to_entities, full_name);
    const auto& entities_to_vals = FindOrDie(metric_to_entities_, full_name);
    int64_t sum = 0;
    // Add everything that the visitor already knows about that isn't stale.
    for (const auto& e : entities_to_vals) {
      if (!ContainsKey(mr_entities_to_vals, e.first)) {
        DCHECK(e.second.value_);
        sum += *e.second.value_;
      }
    }
    // Add all of the values from the new record.
    for (const auto& e : mr_entities_to_vals) {
      DCHECK(e.second.value_);
      sum += *e.second.value_;
    }
    cout << Substitute("\t$0", sum);
  }

  // TODO(awong): there's a fair amount of code duplication here. Clean it up.
  for (const auto& full_and_display : opts_.rate_metric_names) {
    if (last_visited_timestamp_ == 0) {
      cout << "\t0";
      continue;
    }
    const auto& full_name = full_and_display.first;
    const auto& mr_entities_to_vals = FindOrDie(mr.metric_to_entities, full_name);
    const auto& entities_to_vals = FindOrDie(metric_to_entities_, full_name);
    int64_t sum = 0;
    int64_t prev_sum = 0;
    // Add everything that the visitor already knows about that isn't stale.
    for (const auto& e : entities_to_vals) {
      DCHECK(e.second.value_);
      prev_sum += *e.second.value_;
      if (!ContainsKey(mr_entities_to_vals, e.first)) {
        sum += *e.second.value_;
      }
    }
    // Add all of the values from the new record.
    for (const auto& e : mr_entities_to_vals) {
      DCHECK(e.second.value_);
      sum += *e.second.value_;
    }
    cout << Substitute("\t$0",
        double(sum - prev_sum) / ((mr.timestamp - last_visited_timestamp_) / 1e6));
  }

  for (const auto& full_and_display : opts_.hist_metric_names) {
    const auto& full_name = full_and_display.first;
    const auto& mr_entities_to_vals = FindOrDie(mr.metric_to_entities, full_name);
    const auto& entities_to_vals = FindOrDie(metric_to_entities_, full_name);
    std::map<int64_t, int> all_counts;
    for (auto& e : entities_to_vals) {
      DCHECK(e.second.counts_);
      if (!ContainsKey(mr_entities_to_vals, e.first)) {
        MergeTokenCounts(&all_counts, *e.second.counts_);
      }
    }
    for (auto& e : mr_entities_to_vals) {
      DCHECK(e.second.counts_);
      MergeTokenCounts(&all_counts, *e.second.counts_);
    }
    int counts_size = 0;
    for (const auto& vals_and_counts : all_counts) {
      counts_size += vals_and_counts.second;
    }

    // TODO(awong): would using HdrHistogram be cleaner?
    const vector<int> quantiles = { 0,
                                    counts_size / 2,
                                    counts_size * 75 / 100,
                                    counts_size * 99 / 100,
                                    counts_size - 1 };
    int quantile_idx = 0;
    int count_idx = 0;
    for (const auto& vals_and_counts : all_counts) {
      count_idx += vals_and_counts.second;
      while (quantile_idx < quantiles.size() && quantiles[quantile_idx] <= count_idx) {
        cout << Substitute("\t$0", vals_and_counts.first);
        quantile_idx++;
      }
    }
  }

  cout << std::endl;
  // Update the visitor's metrics.
  for (const auto& metric_and_enties_map : mr.metric_to_entities) {
    const string& metric_name = metric_and_enties_map.first;

    // Update the visitor's internal map with the metrics from the record.
    // Note: The metric record should only have parsed user-requested metrics
    // that the visitor has in its internal map, so we can FindOrDie here.
    auto& visitor_entities_map = FindOrDie(metric_to_entities_, metric_name);
    const auto& mr_entities_map = metric_and_enties_map.second;
    for (const auto& mr_entity_and_val : mr_entities_map) {
      const string& entity_id = mr_entity_and_val.first;
      InsertOrUpdate(&visitor_entities_map, entity_id, mr_entity_and_val.second);
    }
  }
  last_visited_timestamp_ = mr.timestamp;
  return Status::OK();
}

Status MetricCollectingLogVisitor::ParseRecord(const ParsedLine& pl) {
  // Do something with the metric record.
  if (pl.type() == RecordType::kMetrics) {
    MetricsRecord mr;
    RETURN_NOT_OK(mr.FromParsedLine(opts_, std::move(pl)));
    RETURN_NOT_OK(VisitMetricsRecord(std::move(mr)));
  }
  return Status::OK();
}

Status StackDumpingLogVisitor::ParseRecord(const ParsedLine& pl) {
  // We're not going to do any fancy parsing, so do it up front with the deault
  // json parsing.
  switch (pl.type()) {
    case RecordType::kSymbols: {
      SymbolsRecord sr;
      RETURN_NOT_OK(sr.FromParsedLine(std::move(pl)));
      VisitSymbolsRecord(sr);
      break;
    }
    case RecordType::kStacks: {
      StacksRecord sr;
      RETURN_NOT_OK(sr.FromParsedLine(std::move(pl)));
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
  const string error_msg = Substitute("Failed to parse line $0 in file $1",
                                      line_number_, path_);
  RETURN_NOT_OK_PREPEND(pl.Parse(), error_msg);
  RETURN_NOT_OK_PREPEND(log_visitor_->ParseRecord(std::move(pl)), error_msg);
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

