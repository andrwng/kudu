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

#pragma once

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/errno.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

// One of the record types from the log.
// TODO(KUDU-2353) support metrics records.
enum class RecordType {
  kSymbols,
  kStacks,
  kMetrics,
  kUnknown
};

const char* RecordTypeToString(RecordType r);

std::ostream& operator<<(std::ostream& o, RecordType r);

// A parsed line from the diagnostics log.
//
// Each line contains a timestamp, a record type, and some JSON data.
class ParsedLine {
 public:
  ParsedLine(std::string line)
    : line_(std::move(line)) {}

  // Parse a line from the diagnostics log.
  Status Parse();

  Status ParseHeader();
  Status ParseJson();

  RecordType type() const { return type_; }

  const rapidjson::Value* json() const {
    CHECK(json_);
    return json_->root();
  }

  std::string date_time() const;

  int64_t timestamp() const { return timestamp_; }

 private:
  const std::string line_;
  RecordType type_;

  // date_ and time_ point to substrings of line_.
  StringPiece date_;
  StringPiece time_;
  int64_t timestamp_;

  // A JsonReader initialized from the most recent line.
  // This will be 'none' before any lines have been read.
  boost::optional<JsonReader> json_;
};

// A stack sample from the log.
struct StacksRecord {
  // A group of threads which share the same stack trace.
  struct Group {
    // The thread IDs in this group.
    std::vector<int> tids;
    // The non-symbolized addresses forming the stack trace.
    std::vector<std::string> frame_addrs;
  };

  Status FromParsedLine(const ParsedLine& pl);

  // The time the stack traces were collected.
  std::string date_time;

  // The reason for stack trace collection.
  std::string reason;

  // The grouped threads with their stack traces.
  std::vector<Group> groups;
};

// Interface for consuming the parsed records from a diagnostics log.
class LogVisitor {
 public:
  virtual ~LogVisitor() {}
  virtual Status ParseRecord(const ParsedLine& pl) = 0;
};

enum class MetricType {
  kUninitialized,
  kPlain,
  kHistogram,
};

class MetricValue {
 public:
  Status FromJson(const rapidjson::Value& metric_json);
  Status MergeMetric(const MetricValue& v);
  MetricType type() const;
  Status CheckMatchingType(const MetricValue& v);
 protected:
  MetricType type_;

  boost::optional<int64_t> value_;
  boost::optional<std::vector<int64_t, int64_t>> counts_;
};

// For a given metric, a collection of entity ids and their metric value.
typedef std::unordered_map<std::string, MetricValue> EntityIdToValue;

// Mapping from a full metric name to the collection of entity ids and their
// metric values.
typedef std::unordered_map<std::string, EntityIdToValue> MetricToEntities;

// A full metric name, i.e. <entity>.<metric name>
typedef std::pair<std::string, std::string> FullMetricName;

struct MetricsCollectingOpts {
  // Maps the full metric name to its display name.
  typedef std::unordered_map<FullMetricName, std::string> NameMap;

  // The metric names and display names of the metrics of interest.
  NameMap simple_metric_names;
  NameMap rate_metric_names;
  NameMap hist_metric_names;

  // Set of tablet ids whose metrics that should be aggregated.
  // If empty, all tablets' metrics will be aggregated.
  std::unordered_set<std::string> tablet_ids;

  // The timestamps of interest in microseconds.
  // [inclusive_ts_lower_us, exclusive_ts_upper_us)
  int64_t inclusive_ts_lower_us;
  int64_t exclusive_ts_upper_us;
};

// Aggregates metrics, potentially across multiple lines or files.
// A single MetricsCollector may be used by multiple FileParsers.
//
// Needs to see every timestamp, even if it's not relevant to the query, since
// rates will need to take into account all timestamps.
//
// Design goal: keep the contents as small as possible.
struct MetricsRecord {
  Status FromParsedLine(const MetricsCollectingOpts& opts, const ParsedLine& pl);

  // { <entity>.<metric name>:string => { <entity id>:string => <metric value> }
  MetricToEntities metric_to_entities;
};

class MetricCollectingLogVisitor : public LogVisitor {
 public:
  MetricCollectingLogVisitor(MetricsCollectingOpts opts)
    : opts_(std::move(opts)) {}

  Status ParseRecord(const ParsedLine& pl) override;

  void VisitMetricsRecord(const MetricsRecord& mr);
 private:
  // Maps the full metric name to the mapping between entity ids and their
  // metric value. As the visitor visits new metrics records, this gets updated
  // with the most up-to-date values.
  MetricToEntities metric_to_entities_;

  // The timestamp of the last visited metrics record.
  int64_t last_visited_timestamp_ = 0;

  const MetricsCollectingOpts opts_;
};

struct SymbolsRecord {
  Status FromParsedLine(const ParsedLine& pl);
  std::unordered_map<std::string, std::string> addr_to_symbol;
};

// LogVisitor implementation which dumps the parsed stack records to cout.
class StackDumpingLogVisitor : public LogVisitor {
 public:
  Status ParseRecord(const ParsedLine& pl) override;

 private:
  void VisitSymbolsRecord(const SymbolsRecord& sr);
  void VisitStacksRecord(const StacksRecord& sr);
  // True when we have not yet output any data.
  bool first_ = true;
  // Map from symbols to name.
  std::unordered_map<std::string, std::string> symbols_;

  const std::string kUnknownSymbol = "<unknown>";
};

enum class DiagnosticLogVersion {
  // metrics 1519177089950546 <json blob>...
  kVersion1,

  // I0220 17:38:09.950546 metrics 1519177089950546 <json blob>...
  kVersion2,
};

// Parser for a metrics log files.
//
// This instance follows a 'SAX' model. As records are available, the appropriate
// functions are invoked on the visitor provided in the constructor.
class LogFileParser {
 public:
  explicit LogFileParser(LogVisitor* lv, std::string path)
    : path_(std::move(path)),
      fstream_(path_),
      log_visitor_(lv) {}

  Status Init();

  // Peeks at the next line. Doesn't increase the line number.
  // Returns whether or not this was successful.
  std::string PeekNextLine();

  // Returns whether or not the underlying file has more lines to parse.
  bool HasNext();

  // Parses the next line in the file.
  Status ParseLine();

  // Parses the rest of the lines in the file.
  Status Parse();

 private:
  int line_number_ = 0;
  std::string path_;
  std::ifstream fstream_;

  LogVisitor* log_visitor_;
};

} // namespace tools
} // namespace kudu
