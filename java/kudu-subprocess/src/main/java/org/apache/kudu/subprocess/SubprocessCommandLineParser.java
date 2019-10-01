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

package org.apache.kudu.subprocess;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Utility class that parse command line interface for starting a subprocess.
 */
@InterfaceAudience.Private
class SubprocessCommandLineParser {
  private String[] args;

  private static final String QUEUE_SIZE_SHORT_OPTION = "q";
  private static final String QUEUE_SIZE_LONG_OPTION = "queuesize";
  private static final boolean QUEUE_SIZE_HAS_ARG = true;
  private static final String QUEUE_SIZE_DESC = "Size of the message queue for subprocess";

  private static final String MAX_WRITER_THREADS_SHORT_OPTION = "w";
  private static final String MAX_WRITER_THREADS_LONG_OPTION = "maxWriterThreads";
  private static final boolean MAX_WRITER_THREADS_HAS_ARG = true;
  private static final String MAX_WRITER_THREADS_DESC =
      "Maximum number of threads in the writer thread pool for subprocess";

  private static final String MAX_MESSAGE_BYTES_SHORT_OPTION = "m";
  private static final String MAX_MESSAGE_BYTES_LONG_OPTION = "maxMsgBytes";
  private static final boolean MAX_MESSAGE_BYTES_HAS_ARG = true;
  private static final String MAX_MESSAGE_BYTES_DESC =
      "Maximum bytes of a message for subprocess";

  public SubprocessCommandLineParser(String[] args) {
    this.args = args;
  }

  /**
   * Parses the arguments according to the specified options.
   *
   * @return the subprocess configuration
   * @throws KuduSubprocessException if there are any problems encountered
   *                                 while parsing the command line interface.
   */
  public SubprocessConfiguration parse() throws KuduSubprocessException {
    SubprocessConfiguration conf;
    Options options = new Options();
    Option queueSizeOpt = new Option(QUEUE_SIZE_SHORT_OPTION,
                                     QUEUE_SIZE_LONG_OPTION,
                                     QUEUE_SIZE_HAS_ARG,
                                     QUEUE_SIZE_DESC);
    queueSizeOpt.setRequired(false);
    options.addOption(queueSizeOpt);

    Option maxThreadsOpt = new Option(MAX_WRITER_THREADS_SHORT_OPTION,
                                      MAX_WRITER_THREADS_LONG_OPTION,
                                      MAX_WRITER_THREADS_HAS_ARG,
                                      MAX_WRITER_THREADS_DESC);
    maxThreadsOpt.setRequired(false);
    options.addOption(maxThreadsOpt);

    Option maxMsgOpt = new Option(MAX_MESSAGE_BYTES_SHORT_OPTION,
                                  MAX_MESSAGE_BYTES_LONG_OPTION,
                                  MAX_MESSAGE_BYTES_HAS_ARG,
                                  MAX_MESSAGE_BYTES_DESC);
    maxMsgOpt.setRequired(false);
    options.addOption(maxMsgOpt);

    CommandLineParser parser = new BasicParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      String queueSize = cmd.getOptionValue(QUEUE_SIZE_LONG_OPTION);
      String maxWriterThreads = cmd.getOptionValue(MAX_WRITER_THREADS_LONG_OPTION);
      String maxMsgBytes = cmd.getOptionValue(MAX_MESSAGE_BYTES_LONG_OPTION);
      conf = new SubprocessConfiguration(queueSize, maxWriterThreads, maxMsgBytes);
    } catch (ParseException e) {
      throw new KuduSubprocessException("Cannot parse the subprocess command line", e);
    }
    return conf;
  }
}
