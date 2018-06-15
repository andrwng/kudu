#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This script parses a set of metrics logs output from a tablet server,
and outputs a TSV file including some metrics.

This isn't meant to be used standalone as written, but rather as a template
which is edited based on whatever metrics you'd like to extract. The set
of metrics described below are just a starting point to work from.
Uncomment the ones you are interested in, or add new ones.
"""

from collections import Counter
import gzip
import heapq
import itertools
try:
  import simplejson as json
except:
  import json
import sys

# Even if the input data has very frequent metrics, for graphing purposes we
# may not want to look at such fine-grained data. This constant can be set
# to drop samples which were measured within a given number of seconds
# from the prior sample.
GRANULARITY_SECS = 30

# These metrics will be extracted "as-is" into the TSV.
# The first element of each tuple is the metric name.
# The second is the name that will be used in the TSV header line.
SIMPLE_METRICS = [
  #  ("server.generic_current_allocated_bytes", "heap_allocated"),
  #  ("server.log_block_manager_bytes_under_management", "bytes_on_disk"),
  #  ("tablet.memrowset_size", "mrs_size"),
  #  ("server.block_cache_usage", "bc_usage"),
]

# These metrics will be extracted as per-second rates into the TSV.
RATE_METRICS = [
  #  ("server.block_manager_total_bytes_read", "bytes_r_per_sec"),
  #  ("server.block_manager_total_bytes_written", "bytes_w_per_sec"),
  #  ("server.block_cache_lookups", "bc_lookups_per_sec"),
  #  ("server.cpu_utime", "cpu_utime"),
  #  ("server.cpu_stime", "cpu_stime"),
  #  ("server.involuntary_context_switches", "invol_cs"),
  #  ("server.voluntary_context_switches", "vol_cs"),
  #  ("tablet.rows_inserted", "inserts_per_sec"),
  #  ("tablet.rows_upserted", "upserts_per_sec"),
]

# These metrics will be extracted as percentile metrics into the TSV.
# Each metric will generate several columns in the output TSV, with
# percentile numbers suffixed to the column name provided here (foo_p95,
# foo_p99, etc)
HISTOGRAM_METRICS = [
  #  ("server.op_apply_run_time", "apply_run_time"),
  #  ("server.handler_latency_kudu_tserver_TabletServerService_Write", "write"),
  #  ("server.handler_latency_kudu_consensus_ConsensusService_UpdateConsensus", "cons_update"),
  #  ("server.handler_latency_kudu_consensus_ConsensusService_RequestVote", "vote"),
  #  ("server.handler_latency_kudu_tserver_TabletCopyService_FetchData", "fetch_data"),
  #  ("tablet.bloom_lookups_per_op", "bloom_lookups"),
  #  ("tablet.log_append_latency", "log"),
  #  ("tablet.op_prepare_run_time", "prep"),
  #  ("tablet.write_op_duration_client_propagated_consistency", "op_dur")
]

NaN = float('nan')
UNKNOWN_PERCENTILES = dict(p50=0, p95=0, p99=0, p999=0, max=0)

def merge_delta(m, delta):
  """
  Update (in-place) the metrics entry 'm' by merging another entry 'delta'.

  Counts and sums are simply added.
  Histograms require more complex processing: the 'values' array needs to be
  merged and then the delta's counts added to the corresponding buckets.
  """

  for k, v in delta.iteritems():
    if k in ('name', 'values', 'counts', 'min', 'max', 'mean'):
      continue
    m[k] += v

  # Merge counts.
  if 'counts' in delta:
    print m.get('values', []), m.get('counts', [])
    print delta.get('values', []), delta.get('counts', [])
    m_zip = itertools.izip(m.get('values', []), m.get('counts', []))
    d_zip = itertools.izip(delta.get('values', []), delta.get('counts', []))
    new_values = []
    new_counts = []
    merged = itertools.groupby(heapq.merge(m_zip, d_zip), lambda x: x[0])
    for value, counts in merged:
      new_values.append(value)
      new_counts.append(sum(c for v, c in counts))
    m['counts'] = new_counts
    m['values'] = new_values
    print "merged", m['values'], m['counts']


def strip_metric(m):
  """
  Strip the input metric string, returning only the values and counts for the
  metric.
  """
  if 'value' in m:
    return {'value' : m['value'] }

  if 'counts' in m:
    return { 'counts': m['counts'], 'values': m['values'] }


def json_to_map(j, metric_keys):
  """
  Parse the JSON structure in the log into a python dictionary
  keyed by <entity>.<metric name>.

  The entity ID is currently ignored. If there is more than one
  entity of a given type (eg tablets), the metrics will be summed
  together using 'merge_delta' above.
  """
  ret = {}
  for entity in j:
    for m in entity['metrics']:
      entity_id = entity['id']
      key = entity['type'] + "." + m['name']
      if key not in metric_keys:
        continue
      if key in ret:
        # Add the metric_id to the metrics map.
        ret[key][entity_id] = strip_metric(m)
      else:
        # Create an entry for the metrics map.
        ret[key] = { entity_id : strip_metric(m) }
  return ret

def delta(prev, cur, m):
  """ Compute the delta in metric 'm' between two metric snapshots. """
  # Update the current metric in case we have missing entries.
  return sum([val['value'] for eid, val in cur[m].iteritems()]) - \
      sum([val['value'] for eid, val in prev[m].iteritems()])

def aggregate_metrics(m):
  """ Aggregates metrics across entity ids """
  ret = {}
  for metric_name in m:
    if metric_name == "ts":
      continue
    # Iterate through all the entities in for this metric.
    for eid in m:
      # Add singular values.
      if 'value' in m[eid]:
        if metric_name in ret:
          ret += m[eid]
        else:
          ret = m[eid]
      # Combine histogram values.
      elif 'values' in m[eid]:
        # Merge all of the counts.
        added_hist = dict(zip(m[eid].get('values', []),
                              m[eid].get('counts', [])))
        for val, count in added_hist.iteritems():
          if val in ret:
            ret[val] += count
          else:
            ret[val] = count
  return ret

def histogram_stats(prev, cur, m):
  """
  Compute percentile stats for the metric 'm' in the window between two
  metric snapshots.
  """
  if not m in prev:
    return UNKNOWN_PERCENTILES

  p_dict = aggregate_metrics(prev[m])
  c_dict = aggregate_metrics(cur[m])

  # Determine the total count we should expect between the current and previous
  # snapshots.
  window_total = sum([val for _, val in c_dict.iteritems()]) - \
      sum([val for _, val in p_dict.iteritems()])

  if window_total == 0:
    return UNKNOWN_PERCENTILES
  res = dict()
  cum_count = 0

  # Iterate over all of the buckets for the current and previous snapshots,
  # summing them up, and assigning percentiles to the bucket as appropriate.
  for cur_val, cur_count in sorted(c_dict.iteritems()):
    prev_count = p_dict.get(cur_val, 0)

    # Determine the total count for this bucket across the current and the
    # previous snapshot.
    bucket_count = cur_count - prev_count
    cum_count += bucket_count

    # Determine which percentiles this bucket belongs to.
    percentile = float(cum_count) / window_total
    if 'p50' not in res and percentile > 0.50:
      res['p50'] = cur_val
    if 'p95' not in res and percentile > 0.95:
      res['p95'] = cur_val
    if 'p99' not in res and percentile > 0.99:
      res['p99'] = cur_val
    if 'p999' not in res and percentile > 0.999:
      res['p999'] = cur_val
    if cum_count == window_total:
      res['max'] = cur_val
  return res

def cache_hit_ratio(prev, cur):
  """
  Calculate the cache hit ratio between the two samples.
  If there were no cache hits or misses, this returns NaN.
  """
  delta_hits = delta(prev, cur, 'server.block_cache_hits_caching')
  delta_misses = delta(prev, cur, 'server.block_cache_misses_caching')
  if delta_hits + delta_misses > 0:
    cache_ratio = float(delta_hits) / (delta_hits + delta_misses)
  else:
    cache_ratio = NaN
  return cache_ratio

def process(prev, cur,
            simple_metrics=SIMPLE_METRICS,
            rate_metrics=RATE_METRICS,
            histogram_metrics=HISTOGRAM_METRICS):
  """ Process a pair of metric snapshots, outputting a line of TSV. """
  delta_ts = cur['ts'] - prev['ts']
  cache_ratio = cache_hit_ratio(prev, cur)
  calc_vals = []
  for metric, _ in simple_metrics:
    if not metric in cur:
      if metric in prev:
        # The diagnostics log will only report metrics that have changed, so if
        # we don't see a metric, assign it to its previous value.
        cur[metric] = { 'value': prev[metric]['value'] }
      else:
        # The above means we may not see metrics that have remained 0 since the
        # start of the server, so assign metrics that we haven't seen to 0.
        #
        # Note: this means that attempting to process metrics that don't exist
        # will also yield plots that show all 0.
        cur[metric]['value'] = 0
    calc_vals.append(cur[metric]['value'])

  calc_vals.extend(delta(prev, cur, metric)/delta_ts for (metric, _) in rate_metrics)
  for metric, _ in histogram_metrics:
    stats = histogram_stats(prev, cur, metric)
    calc_vals.extend([stats['p50'], stats['p95'], stats['p99'], stats['p999'], stats['max']])

  return tuple([(cur['ts'] + prev['ts'])/2, cache_ratio] + calc_vals)

class MetricsLogIterator(object):
  def __init__(self, paths,
               min_interval_secs=GRANULARITY_SECS,
               simple_metrics=SIMPLE_METRICS,
               rate_metrics=RATE_METRICS,
               histogram_metrics=HISTOGRAM_METRICS):
    self.paths = paths
    self.min_interval_secs = min_interval_secs
    self.metric_keys = set(key for key, _ in simple_metrics + rate_metrics + histogram_metrics)
    self.metric_keys.add("server.block_cache_hits_caching")
    self.metric_keys.add("server.block_cache_misses_caching")

    self.simple_metrics = simple_metrics
    self.rate_metrics = rate_metrics
    self.histogram_metrics = histogram_metrics

    self.column_names = ["time", "cache_hit_ratio"]
    self.column_names.extend([header for _, header in simple_metrics + rate_metrics])
    for _, header in histogram_metrics:
      self.column_names.append(header + "_p50")
      self.column_names.append(header + "_p95")
      self.column_names.append(header + "_p99")
      self.column_names.append(header + "_p999")
      self.column_names.append(header + "_max")

  # Each iter of the log iterator returns a tuple of the form:
  #   (timestamp, cache ratio, [metric_val])
  def __iter__(self):
    prev_data = None
    for path in sorted(self.paths):
      if path.endswith(".gz"):
        f = gzip.GzipFile(path)
      else:
        f = file(path)
      for line_number, line in enumerate(f, start=1):
        # Only parse out the "metrics" lines.
        try:
          (_, _, log_type, ts, metrics_json) = line.split()
        except ValueError:
          continue
        if not log_type == "metrics":
          continue
        ts = float(ts) / 1000000.0
        prev_ts = prev_data['ts'] if prev_data else 0
        # Enforce that the samples come in time-sorted order.
        if ts <= prev_ts:
          raise Exception("timestamps must be in ascending order (%f <= %f at %s:%d)"
                          % (ts, prev_ts, path, line_number))
        if prev_data and ts < prev_ts + self.min_interval_secs:
          continue
        # This map is { metric key => { entity id => metric } }
        data = json_to_map(json.loads(metrics_json), self.metric_keys)

        # Merge with the previous one, pulling any missing entries from the
        # previous metric map.

        data['ts'] = ts
        if prev_data:
          # Copy missing metrics from prev_data.
          for m, prev_eid_to_vals in prev_data.iteritems():
            if m == 'ts':
              continue

            # The metric was missing entirely; copy it over.
            if m not in data:
              data[m] = prev_eid_to_vals
            else:
              # The metric was missing for a specific entity. Copy the metric
              # from the previous snapshot.
              for eid, prev_vals in prev_eid_to_vals.iteritems():
                if eid not in data[m]:
                  data[m][eid] = prev_vals

          print "prev", prev_data
          print "data", data
          yield process(prev_data, data,
                        self.simple_metrics,
                        self.rate_metrics,
                        self.histogram_metrics)
        prev_data = data

def main(argv):
  m = MetricsLogIterator(argv[1:])
  for row in m:
    print " ".join(str(x) for x in row)
  print " ".join(m.column_names)

if __name__ == "__main__":
  main(sys.argv)
