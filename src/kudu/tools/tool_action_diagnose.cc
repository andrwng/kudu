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

#include <algorithm>
#include <array>
#include <cerrno>
#include <fstream> // IWYU pragma: keep
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/diagnostics_log_parser.h"
#include "kudu/tools/tool_action.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

using std::array;
using std::ifstream;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Split;
using strings::Substitute;

const char* kLogPathArg = "path";
const char* kGlobList = "globs";
const char* kHistogramMetricsList = "histogram_metrics";
const char* kRateMetricsList = "rate_metrics";
const char* kSimpleMetricsList = "simple_metrics";
const char* kTabletList = "tablet_ids";

DEFINE_string(tablets, "",
              "Comma-separated list of table ids to aggregate tablet metrics "
              "across. Defaults to aggregating all tablets.");
DEFINE_string(simple_metrics, "",
              "Comma-separated list of metrics to parse, of the format "
              "<entity>.<metric name>:<display name>, where <entity> must be "
              "either 'server' or 'tablet', <metric name> refers to the "
              "Kudu metric name, and <display name> is what the metric will "
              "be output as.");
DEFINE_string(rate_metrics, "",
              "Comma-separated list of metrics to compute the rate of.");
DEFINE_string(histogram_metrics, "",
              "Comma-separated list of histogram metrics to parse percentiles "
              "for");
namespace {

Status ParseStacksFromPath(StackDumpingLogVisitor* dlv, const string& path) {
  LogParser fp(dlv, path);
  RETURN_NOT_OK(fp.Init());
  return fp.Parse();
}

Status ParseStacks(const RunnerContext& context) {
  vector<string> paths = context.variadic_args;
  // The file names are such that lexicographic sorting reflects
  // timestamp-based sorting.
  std::sort(paths.begin(), paths.end());
  StackDumpingLogVisitor dlv;
  for (const auto& path : paths) {
    RETURN_NOT_OK_PREPEND(ParseStacksFromPath(&dlv, path),
                          Substitute("failed to parse stacks from $0", path));
  }
  return Status::OK();
}

struct MetricNameParams {
  string entity;
  string metric_name;
  string display_name;
};

// Parses a metric parameter of the form <entity>.<metric>:<display name>.
Status SplitMetricNameParams(const string& name_str, MetricNameParams* out) {
  static std::regex kMetricRegex("^([:alpha:]+)\\.([:alpha:]+)\\:([:alpha:]+)$");
  std::smatch match;
  if (!std::regex_search(name_str, match, kMetricRegex) || match.size() != 4) {
    return Status::InvalidArgument(Substitute("Could not parse metric "
       "parameter. Expected <entity>.<metric>:<display name>, got $0", name_str));
  }
  *out = { match[1], match[2], match[3] };
  return Status::OK();
}

// Splits the input string by ','.
vector<string> SplitOnComma(const string& str) {
  return Split(str, ",", strings::SkipEmpty());
}

// Takes a metric name parameter string and inserts the metric names into the
// name map.
Status AddMetricsToDisplayNameMap(const string& metric_params_str,
                                  MetricsCollectingOpts::NameMap* name_map) {
  if (metric_params_str.empty()) {
    return Status::OK();
  }
  vector<string> metric_params = SplitOnComma(metric_params_str);
  for (const auto& metric_param : metric_params) {
    MetricNameParams params;
    RETURN_NOT_OK(SplitMetricNameParams(metric_param, &params));
    const string& entity = params.entity;
    const string& metric_name = params.metric_name;
    if (entity != "server" && entity != "tablet") {
      return Status::InvalidArgument(Substitute("Unexpected entity type $0", entity));
    }
    const FullMetricName full_name = { entity, metric_name };
    if (!EmplaceIfNotPresent(name_map, full_name, std::move(params.display_name))) {
      return Status::InvalidArgument(
          Substitute("Duplicate metric name for $0.$1", entity, metric_name));
    }
  }
  return Status::OK();
}

Status ParseMetrics(const RunnerContext& context) {
  const unordered_map<string, string> args = context.required_args;
  // Parse the locations of the diagnostics files.
  vector<string> paths;
  for (const auto& glob : SplitOnComma(FindOrDie(args, kGlobList))) {
    RETURN_NOT_OK(Env::Default()->Glob(glob, &paths));
  }

  // Parse the metric name parameters.
  MetricsCollectingOpts opts;
  RETURN_NOT_OK(AddMetricsToDisplayNameMap(FLAGS_simple_metrics,
                &opts.simple_metric_names));
  RETURN_NOT_OK(AddMetricsToDisplayNameMap(FLAGS_rate_metrics,
                &opts.rate_metric_names));
  RETURN_NOT_OK(AddMetricsToDisplayNameMap(FLAGS_histogram_metrics,
                &opts.hist_metric_names));

  // Parse the tablet ids.
  if (!FLAGS_tablets.empty()) {
    vector<string> tablet_ids = SplitOnComma(FLAGS_tablets);
    opts.tablet_ids.emplace(tablet_ids.begin(), tablet_ids.end());
  }

  // Sort the files lexicographically to put them in increasing timestamp order.
  std::sort(paths.begin(), paths.end());
  MetricCollectingLogVisitor mlv(std::move(opts));;
  for (const auto& path : paths) {
    LogParser lp(&mlv, path);
    RETURN_NOT_OK(lp.Init());
    lp.Parse();
  }
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildDiagnoseMode() {
  unique_ptr<Action> parse_stacks =
      ActionBuilder("parse_stacks", &ParseStacks)
      .Description("Parse sampled stack traces out of a diagnostics log")
      .AddRequiredVariadicParameter({ kLogPathArg, "path to log file(s) to parse" })
      .Build();

  // TODO: add timestamp bounds
  // TODO: add more sophisticated histogram generation
  unique_ptr<Action> parse_metrics =
      ActionBuilder("parse_metrics", &ParseMetrics)
      .Description("Parse metrics out of a diagnostics log")
      .AddRequiredParameter({ kGlobList,
          "Comma-separated list of globs to log file(s) to parse" })
      .AddOptionalParameter("tablets")
      .AddOptionalParameter("simple_metrics")
      .AddOptionalParameter("rate_metrics")
      .AddOptionalParameter("histogram_metrics")
      .Build();

  return ModeBuilder("diagnose")
      .Description("Diagnostic tools for Kudu servers and clusters")
      .AddAction(std::move(parse_stacks))
      .Build();
}

} // namespace tools
} // namespace kudu
