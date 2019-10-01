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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.WireProtocol.AppStatusPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessRequestPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessResponsePB;

/**
 * The {@link MessageWriter} class,
 *    1. retrieves a message from the queue at a time,
 *    2. processes the message and generates a response,
 *    3. and then writes the response to the underlying output stream.
 */
@InterfaceAudience.Private
class MessageWriter implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MessageWriter.class);
  private static final String PARSE_MSG_ERR = "Unable to parse the protobuf message";
  private static final String TAKE_MSG_ERR = "Unable to take a message from the queue";

  @VisibleForTesting
  static final String WRITE_MSG_ERR = "Unable to write the protobuf message";

  private final BlockingQueue<byte[]> inboundQueue;
  private final MessageIO messageIO;
  private final ProtocolHandler protocolHandler;

  MessageWriter(BlockingQueue<byte[]> inboundQueue,
                MessageIO messageIO,
                ProtocolHandler protocolHandler) {
    Preconditions.checkNotNull(inboundQueue);
    this.inboundQueue = inboundQueue;
    this.messageIO = messageIO;
    this.protocolHandler = protocolHandler;
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      // Take an element from the queue. If encountered InterruptedException,
      // consider it to be fatal (as a signal to shutdown the task), and
      // propagate it the code higher up on the call stack.
      byte[] data;
      try {
        data = inboundQueue.take();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Message: {} has been taken from the queue", data);
        }
      } catch (InterruptedException e) {
        throw new KuduSubprocessException(TAKE_MSG_ERR, e);
      }

      SubprocessResponsePB response = getResponse(data);
      writeResponse(response);
    }
  }

  /**
   * Constructs a message with the given error status.
   *
   * @param errorCode the given error status
   * @param res the message builder
   * @return a message with the given error status
   */
  static SubprocessResponsePB responseWithError(AppStatusPB.ErrorCode errorCode,
                                                SubprocessResponsePB.Builder res) {
    Preconditions.checkNotNull(res);
    AppStatusPB.Builder errorBuilder = AppStatusPB.newBuilder();
    errorBuilder.setCode(errorCode);
    res.setError(errorBuilder);
    return res.build();
  }

  /**
   * Parses the given protobuf message. If encountered InvalidProtocolBufferException,
   * which indicates the message is invalid, respond with an error message.
   *
   * @param data the protobuf message
   * @return a SubprocessResponsePB
   */
  private SubprocessResponsePB getResponse(byte[] data) {
    SubprocessResponsePB response;
    SubprocessResponsePB.Builder responseBuilder = SubprocessResponsePB.newBuilder();
    try {
      // Parses the data as a message of SubprocessRequestPB type.
      SubprocessRequestPB request = SubprocessRequestPB.parser().parseFrom(data);
      response = protocolHandler.handleRequest(request);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn(String.format("%s: %s", PARSE_MSG_ERR,
                             new String(data, StandardCharsets.UTF_8)), e);
      response = responseWithError(AppStatusPB.ErrorCode.ILLEGAL_STATE, responseBuilder);
    }
    return response;
  }

  /**
   * Writes the response to the underlying output stream. IOException is fatal,
   * and should be propagated to the code higher up on the call stack.
   *
   * @param response the response
   */
  private void writeResponse(SubprocessResponsePB response) {
    try {
      messageIO.writeMessage(response);
    } catch (IOException e) {
      throw new KuduSubprocessException(WRITE_MSG_ERR, e);
    }
  }
}
