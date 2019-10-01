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

import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for subprocess that handles EchoRequest messages in various conditions.
 */
public class TestEchoSubprocess {
  private static final Logger LOG = LoggerFactory.getLogger(TestEchoSubprocess.class);

  public static class PrintStreamWithIOException extends PrintStream {
    public PrintStreamWithIOException(OutputStream out) {
      super(out);
    }

    @Override
    public boolean checkError() {
      return true;
    }
  }

  void runEchoSubprocess(InputStream in,
                         PrintStream out,
                         String[] args,
                         Function<Throwable, Object> errorHandler,
                         boolean injectInterrupt)
      throws InterruptedException, ExecutionException, TimeoutException {
    System.setIn(in);
    System.setOut(out);
    SubprocessExecutor subprocessExecutor = new SubprocessExecutor(errorHandler);
    EchoProtocolHandler protocolProcessor = new EchoProtocolHandler();
    if (injectInterrupt) {
      subprocessExecutor.interrupt();
    }
    subprocessExecutor.run(args, protocolProcessor, /* timeoutMs= */1000);
  }

  @Rule
  public RetryRule retryRule = new RetryRule();

  /**
   * Parses non-malformed message should exit normally without any exceptions.
   */
  @Test
  public void testBasicMsg() throws Exception {
    final String message = "data";
    byte[] messageBytes = MessageTestUtil.serializeMessage(
        MessageTestUtil.createEchoSubprocessRequest(message));
    final InputStream in = new ByteArrayInputStream(messageBytes);
    final PrintStream out = new PrintStream(new ByteArrayOutputStream());
    final String[] args = {""};
    final Function<Throwable, Object> noErrors = e -> {
      LOG.error(String.format("Unexpected error: %s", e.getMessage()));
      Assert.assertTrue(false);
      return null;
    };
    final boolean injectInterrupt = false;
    try {
      runEchoSubprocess(in, out, args, noErrors, injectInterrupt);
      fail();
    }  catch (TimeoutException e) {
      // Pass through as writer tasks should continue execution until timeout if not
      // encountered any fatal exceptions.
    }
  }

  /**
   * Parses message with empty payload should exit normally without any exceptions.
   */
  @Test
  public void testMsgWithEmptyPayload() throws Exception {
    byte[] emptyPayload = MessageIO.intToBytes(0);
    final InputStream in = new ByteArrayInputStream(emptyPayload);
    final PrintStream out = new PrintStream(new ByteArrayOutputStream());
    final String[] args = {""};
    final Function<Throwable, Object> noErrors = e -> {
      Assert.assertTrue(false);
      return null;
    };
    final boolean injectInterrupt = false;
    try {
      runEchoSubprocess(in, out, args, noErrors, injectInterrupt);
      fail();
    }  catch (TimeoutException e) {
      // Pass through as writer tasks should continue execution until timeout if not
      // encountered any fatal exceptions.
    }
  }

  /**
   * Parses malformed message should cause <code>IOException</code>.
   */
  @Test
  public void testMalformedMsg() throws Exception {
    final byte[] messageBytes = "malformed".getBytes(StandardCharsets.UTF_8);
    final InputStream in = new ByteArrayInputStream(messageBytes);
    final PrintStream out = new PrintStream(new ByteArrayOutputStream());
    final String[] args = {""};
    final Function<Throwable, Object> hasError = e -> {
      Assert.assertTrue(e instanceof KuduSubprocessException);
      return null;
    };
    final boolean injectInterrupt = false;
    try {
      runEchoSubprocess(in, out, args, hasError, injectInterrupt);
      fail();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getMessage().contains(MessageReader.READ_MSG_ERR));
    }
  }

  /**
   * Parses message with <code>IOException</code> injected should exit with
   * <code>KuduSubprocessException</code>.
   */
  @Test
  public void testInjectIOException() throws Exception {
    final String message = "data";
    final byte[] messageBytes = MessageTestUtil.serializeMessage(
        MessageTestUtil.createEchoSubprocessRequest(message));
    final InputStream in = new ByteArrayInputStream(messageBytes);
    final PrintStream out = new PrintStreamWithIOException(new ByteArrayOutputStream());
    // Only use one writer task to avoid get TimeoutException instead for
    // writer tasks that haven't encountered any exceptions.
    final String[] args = {"-w", "1"};
    final Function<Throwable, Object> hasError = e -> {
      Assert.assertTrue(e instanceof KuduSubprocessException);
      return null;
    };
    final boolean injectInterrupt = false;
    try {
      runEchoSubprocess(in, out, args, hasError, injectInterrupt);
      fail();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getMessage().contains(MessageWriter.WRITE_MSG_ERR));
    }
  }

  /**
   * Parses message with <code>InterruptedException</code> injected should exit
   * with <code>KuduSubprocessException</code>.
   */
  @Test
  public void testInjectInterruptedException() throws Exception {
    final String message = "data";
    final byte[] messageBytes = MessageTestUtil.serializeMessage(
        MessageTestUtil.createEchoSubprocessRequest(message));
    final InputStream in = new ByteArrayInputStream(messageBytes);
    final PrintStream out = new PrintStream(new ByteArrayOutputStream());
    final String[] args = {""};
    Function<Throwable, Object> hasError = e -> {
      Assert.assertTrue(e instanceof KuduSubprocessException);
      return null;
    };
    final boolean injectInterrupt = true;
    try {
      runEchoSubprocess(in, out, args, hasError, injectInterrupt);
      fail();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getMessage().contains(MessageReader.PUT_MSG_ERR));
    }
  }
}
