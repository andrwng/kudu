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

/**
 * Utility class that manages common configurations to run a subprocess.
 */
@InterfaceAudience.Private
class SubprocessConfiguration {
  private int queueSize;
  private static final int QUEUE_SIZE_DEFAULT = 100;
  private int maxWriterThreads;
  private static final int MAX_WRITER_THREADS_DEFAULT = 3;
  private int maxMsgBytes;
  static final int MAX_MESSAGE_BYTES_DEFAULT = 1024 * 1024;

  public SubprocessConfiguration(String queueSize,
                                 String maxWriterThreads,
                                 String maxMsgBytes) {
    this.queueSize = queueSize == null ?
        QUEUE_SIZE_DEFAULT : Integer.parseInt(queueSize);
    this.maxWriterThreads = maxWriterThreads == null ?
        MAX_WRITER_THREADS_DEFAULT : Integer.parseInt(maxWriterThreads);
    this.maxMsgBytes = maxMsgBytes == null ?
        MAX_MESSAGE_BYTES_DEFAULT :Integer.parseInt(maxMsgBytes);
  }

  /**
   * Returns the size of the message queue. If not provided use the
   * default value.
   *
   * @return the size of the message queue
   */
  public int getQueueSize() {
    return queueSize;
  }

  /**
   * Returns the maximum number of threads in the writer thread pool.
   * If not provided use the default value.
   *
   * @return the maximum number of threads in the writer thread pool
   */
  public int getMaxWriterThreads() {
    return maxWriterThreads;
  }

  /**
   * Returns the maximum bytes of a message. If not provided use the
   * default value.
   *
   * @return the maximum bytes of a message
   */
  public int getMaxMessageBytes() {
    return maxMsgBytes;
  }
}
