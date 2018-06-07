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
This script plots a group of metrics log files from a Kudu server, plotting a
metrics over time across multiple metrics log files.

Example
-------
./plot_metrics.py --glob_paths=/first/server/logs/**,/second/server/logs/** \
  --simple_metrics=tablet.memrowset_size:mrs_size \
  --rate_metrics=tablet.rows_inserted:inserts_per_sec,tablet.log_bytes_logged:log_bytes_per_sec \
  --histogram_metrics=tablet.log_append_latency:log_latency
  --min_interval_secs=30
"""

import argparse
import glob
from matplotlib import pyplot as plt
import numpy
import os
import sys

from parse_metrics_log import MetricsLogIterator


PERCENTILES = ['p50', 'p95', 'p99', 'p999', 'max']

def parse_metrics(metrics_globs,
                  min_interval_secs,
                  simple_metrics,
                  rate_metrics,
                  histogram_metrics):
  """
  Fetches the metrics logs specified by 'metrics_globs' and parses them,
  returning a dictionary of numpy arrays, each containing the metrics
  corresponding to an entry of 'metrics_globs'.
  """
  server_metrics = {}
  for server_metrics_glob in metrics_globs:
    server_glob = glob.glob(server_metrics_glob)
    metrics_iter = MetricsLogIterator(
        list(server_glob),
        min_interval_secs=min_interval_secs,
        simple_metrics=simple_metrics,
        rate_metrics=rate_metrics,
        histogram_metrics=histogram_metrics)

    # Iterate over the metrics iterator via list() and stuff them into a numpy
    # array, assigning all metrics to 'float' types.
    server_metrics[server_metrics_glob] = numpy.array(list(metrics_iter),
        dtype=[(col, float) for col in metrics_iter.column_names])
  print server_metrics
  return server_metrics


def plot_metrics(metric_name, glob_to_metrics):
  """
  Plots the simple or rate metric specified by 'metric_name'.
  'glob_to_metrics' is a map of glob strings to the already-parsed metrics.
  """
  fig = plt.figure(figsize=(20, 5))
  ax = fig.add_subplot(1, 1, 1)
  for glob, metrics in glob_to_metrics.iteritems():
    if len(metrics) == 0:
      continue
    ax.plot(metrics["time"] - min(metrics["time"]), metrics[metric_name])
  ax.legend([g for g in glob_to_metrics])
  ax.set_xlabel("Time (sec)")
  ax.set_ylabel(metric_name)
  plt.show()


def plot_histogram_metrics(histogram_metric_name, glob_to_metrics):
  """
  Plots the histogram metric specified by 'histogram_metric_name'.
  'glob_to_metrics' is a map of glob strings to the already-parsed metrics.
  """
  fig = plt.figure(figsize=(20, 5))
  # Keep track of the bounds across all percentiles for this metric so we can
  # adjust the y-axis.
  max_ylim = None
  min_ylim = None
  for i, p in enumerate(PERCENTILES):
    # Generate subplots, one on top of another.
    ax = fig.add_subplot(1, len(PERCENTILES), i + 1)
    histogram_percentile_name = "{}_{}".format(histogram_metric_name, p)
    for glob, metrics in glob_to_metrics.iteritems():
      if len(metrics) == 0:
        continue
      ax.plot(metrics["time"] - min(metrics["time"]), metrics[histogram_percentile_name])
    ax.set_xlabel("Time (sec)")
    ax.set_ylabel(histogram_percentile_name)
    if not max_ylim and not min_ylim:
      min_ylim = ax.get_ylim()[0]
      max_ylim = ax.get_ylim()[1]
    else:
      min_ylim = min(ax.get_ylim()[0], min_ylim)
      max_ylim = max(ax.get_ylim()[1], max_ylim)

  # Set the y-axis limits so it's easier to compare.
  for ax in fig.get_axes():
    ax.set_ylim((min_ylim, max_ylim))
  plt.show()


def parse_metric_tuples(metrics_str):
  """
  Splits apart a string of the form "<entity>.<metric name>:<label>" and
  returns a tuple ("<entity>.<metric name>", "<label>").
  """
  tuple_strings = metrics_str.split(",")
  tuples = []
  for tuple_string in tuple_strings:
    m = tuple_string.split(":")
    tuples.append((m[0], m[1]))
  return tuples


def main():
  p = argparse.ArgumentParser("Plot metrics from a group of metric logs files")
  p.add_argument("--glob_paths",
      dest="glob_paths", type=str,
      help="""Comma-separated list of globs pointing to diagnostic log files;
      each glob string is expected to refer to a single server's logs.""")
  p.add_argument("--simple_metrics",
      dest="simple_metrics", type=str,
      help="""Comma-separated list of metrics tuples of the form
      <entity>.<metric name>:<shorthand>, where 'entity' refers to the
      Kudu-defined metric entity type, 'metric name' refers to the metric name
      of interest, and 'shorthand' refers to the desired short-hand name for
      the metric when plotting.  The specified metrics will be plotted as they
      are over time.""")
  p.add_argument("--rate_metrics",
      dest="rate_metrics", type=str,
      help="""Comma-separated list of metrics tuples. The rate of change of
      the specified metrics will be computed and plotted over time.""")
  p.add_argument("--histogram_metrics",
      dest="histogram_metrics", type=str,
      help="""Comma-separated list of histogram metric tuples. The histogram
      metrics will be plotted over time, splitting over various
      percentiles.""")
  p.add_argument("--min_interval_secs",
      dest="min_interval_secs", type=str, default="30",
      help="""Minimum time interval required to wait between metrics""")
  args = p.parse_args()
  simple_metrics = []
  if args.simple_metrics:
    simple_metrics = parse_metric_tuples(args.simple_metrics)

  rate_metrics = []
  if args.rate_metrics:
    rate_metrics = parse_metric_tuples(args.rate_metrics)

  histogram_metrics = []
  if args.histogram_metrics:
    histogram_metrics = parse_metric_tuples(args.histogram_metrics)

  # If we don't have any metrics to plot, plot something that might be useful,
  # like the insert rate.
  if len(simple_metrics) + len(rate_metrics) + len(histogram_metrics) == 0:
    rate_metrics = [("tablet.rows_inserted", "inserts_per_sec")]

  glob_paths = args.glob_paths.split(",")

  parsed_metrics = parse_metrics(glob_paths,
                                 float(args.min_interval_secs),
                                 simple_metrics,
                                 rate_metrics,
                                 histogram_metrics)

  for full_name, shorthand in simple_metrics + rate_metrics:
    plot_metrics(shorthand, parsed_metrics)

  for full_name, shorthand in histogram_metrics:
    plot_histogram_metrics(shorthand, parsed_metrics)

if __name__ == "__main__":
  main()
