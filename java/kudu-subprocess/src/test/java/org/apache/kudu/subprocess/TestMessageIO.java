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

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import com.google.common.primitives.Bytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.subprocess.Subprocess.SubprocessRequestPB;
import org.apache.kudu.test.junit.RetryRule;

/**
 * Tests for read and write protobuf message.
 */
public class TestMessageIO {

  @Rule
  public RetryRule retryRule = new RetryRule();

  public static class PrintStreamOverload extends PrintStream {
    public PrintStreamOverload(OutputStream out) {
      super(out);
    }

    @Override
    public void setError() {
      super.setError();
    }
  }

  /**
   * Serializes a subprocess message that wraps EchoRequestPB and de-serializes
   * it to verify the content.
   */
  @Test
  public void testBasicEchoMessage() throws Exception {
    final String data = "data";
    final SubprocessRequestPB request = MessageTestUtil.createEchoSubprocessRequest(data);
    final byte[] message = MessageTestUtil.serializeMessage(request);
    final SubprocessRequestPB actualRequest = MessageTestUtil.deserializeMessage(
        message, SubprocessRequestPB.parser());
    Assert.assertEquals(request, actualRequest);
  }

  /**
   * Verifies that writing messages via <code>SubprocessOutputStream</code> can
   * catch errors thrown from underlying <code>PrintStream</code> and re-throws
   * <code>IOException</code>.
   */
  @Test
  public void testSubprocessOutputStream() {
    //
    final String data = "data";
    final SubprocessRequestPB request = MessageTestUtil.createEchoSubprocessRequest(data);
    final PrintStreamOverload printStreamOverload =
        new PrintStreamOverload(new ByteArrayOutputStream());
    final BufferedOutputStream out = new BufferedOutputStream(
        new SubprocessOutputStream(printStreamOverload));
    final MessageIO messageIO = new MessageIO(
        SubprocessConfiguration.MAX_MESSAGE_BYTES_DEFAULT, /* in= */null, out);
    try {
      printStreamOverload.setError();
      messageIO.writeMessage(request);
      fail();
    } catch (IOException e) {
      Assert.assertThat(e.getMessage(),
                        containsString(SubprocessOutputStream.WRITE_ERR));
    }
  }

  /**
   * Verifies that reading malformed messages should cause expected errors.
   */
  @Test
  public void testMalformedMessage() {
    // Construct a message with the message size exceeds the maximum.
    byte[] size = MessageIO.intToBytes(SubprocessConfiguration.MAX_MESSAGE_BYTES_DEFAULT + 1);
    byte[] body = new byte[0];
    byte[] malformedMessage = Bytes.concat(size, body);
    ByteArrayInputStream byteInputStream = new ByteArrayInputStream(malformedMessage);
    BufferedInputStream in = new BufferedInputStream(byteInputStream);
    MessageIO messageIO = new MessageIO(
        SubprocessConfiguration.MAX_MESSAGE_BYTES_DEFAULT, in, /* out= */null);
    try {
      messageIO.readBytes();
      fail();
    } catch (IOException e) {
      Assert.assertThat(e.getMessage(),
                        containsString("exceeds maximum message size"));
    }

    // Construct a message that has mismatched size and body (not
    // enough data in the body).
    size = MessageIO.intToBytes(100);
    body = new byte[10];
    Arrays.fill(body, (byte)0);
    malformedMessage = Bytes.concat(size, body);
    in = new BufferedInputStream(new ByteArrayInputStream(malformedMessage));
    messageIO = new MessageIO(SubprocessConfiguration.MAX_MESSAGE_BYTES_DEFAULT,
                                            in, /* out= */null);
    try {
      messageIO.readBytes();
      fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("unable to receive message"));
    }
  }
}
