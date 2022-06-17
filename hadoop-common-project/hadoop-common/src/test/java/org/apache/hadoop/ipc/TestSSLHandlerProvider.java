/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ipc;


import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.ssl.SslHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.SSLProtocolException;
import java.io.File;

import static org.apache.hadoop.http.TestSSLHttpServer.EXCLUDED_CIPHERS;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.CLIENT_KEY_STORE_PASSWORD_DEFAULT;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.SERVER_KEY_STORE_PASSWORD_DEFAULT;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.TRUST_STORE_PASSWORD_DEFAULT;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSSLHandlerProvider {

  private static String keystoreDir;
  private static String sslConfDir;

  private static String sslServerConfFile;
  private static String sslClientConfFile;

  String serverCertificatePath;
  String clientCertificatePath;

  private static SSLHandlerProvider sslServerHandlerProvider;
  private static SSLHandlerProvider sslClientHandlerProvider;

  private static Configuration conf;

  private static final String BASEDIR =
      GenericTestUtils.getTempPath(TestSSLHandlerProvider.class.getSimpleName());

  @Before
  public void setup() throws Exception {
    File base = new File(BASEDIR);

    FileUtil.fullyDelete(base);
    base.mkdirs();

    conf = new Configuration();

    keystoreDir = new File(BASEDIR).getAbsolutePath();

    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSSLHandlerProvider.class);

    KeyStoreTestUtil.setupSSLConfig(keystoreDir, sslConfDir, conf,
        false, true,
        EXCLUDED_CIPHERS, SERVER_KEY_STORE_PASSWORD_DEFAULT,
        CLIENT_KEY_STORE_PASSWORD_DEFAULT, TRUST_STORE_PASSWORD_DEFAULT);

    sslServerConfFile = "ssl-server.xml";
    sslClientConfFile = "ssl-client.xml";

    sslServerHandlerProvider =
        new SSLHandlerProvider(sslServerConfFile, "TLS", "SHA1withRSA", false);
    sslClientHandlerProvider =
        new SSLHandlerProvider(sslClientConfFile, "TLS", "SHA1withRSA", true);
  }

  @After
  public void cleanup() throws Exception {
    KeyStoreTestUtil.cleanupSSLConfig(keystoreDir, sslConfDir);
  }

  @Test
  public void testEndtoEnd() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel();

    SslHandler sslServerHandler = sslServerHandlerProvider.getSSLHandler(ch.alloc());
    ch.pipeline().addLast(new StringDecoder());
    ch.pipeline().addLast(sslServerHandler);


    SslHandler sslClientHandler = sslClientHandlerProvider.getSSLHandler(ch.alloc());
    ch.pipeline().addLast(sslClientHandler);
    ch.pipeline().addLast(new StringEncoder());

    ch.writeInbound("hello");
    String output = ch.readInbound();
    assertSame(output, "hello");
  }

  @Test
  public void testTruncatedPacket() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel();
    SslHandler sslHandler = sslServerHandlerProvider.getSSLHandler(ch.alloc());
    ch.pipeline().addLast(sslHandler);

    // Push the first part of a 5-byte handshake message.
    ch.writeInbound(Unpooled.wrappedBuffer(new byte[] {22, 3, 1, 0, 5}));

    // Should decode nothing yet.
    assertThat(ch.readInbound(), is(nullValue()));

    try {
      // Push the second part of the 5-byte handshake message.
      ch.writeInbound(Unpooled.wrappedBuffer(new byte[] {2, 0, 0, 1, 0}));
      fail();
    } catch (DecoderException e) {
      // The pushed message is invalid, so it should raise an exception if it decoded the message correctly.
      assertThat(e.getCause(), is(instanceOf(SSLProtocolException.class)));
    }
  }

  @Test(expected = UnsupportedMessageTypeException.class)
  public void testNonByteBufNotPassThrough() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel();
    SslHandler sslHandler = sslServerHandlerProvider.getSSLHandler(ch.alloc());
    ch.pipeline().addLast(sslHandler);

    ch.writeOutbound(new Object());
  }

  @Test
  public void testIssueReadAfterActiveWriteFlush() throws Exception {
    // the handshake is initiated by channelActive
    new TlsReadTest().test(false);
  }

  @Test
  public void testIssueReadAfterWriteFlushActive() throws Exception {
    // the handshake is initiated by flush
    new TlsReadTest().test(true);
  }

  private static final class TlsReadTest extends ChannelOutboundHandlerAdapter {
    private volatile boolean readIssued;

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
      readIssued = true;
      super.read(ctx);
    }

    public void test(final boolean dropChannelActive) throws Exception {
      EmbeddedChannel ch = new EmbeddedChannel();
      SslHandler sslHandler =
          sslClientHandlerProvider.getSSLHandler(ch.alloc());
      ch.pipeline().addLast(this);
      ch.pipeline().addLast(sslHandler);
      ch.pipeline().addLast(
          new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx)
                throws Exception {
              if (!dropChannelActive) {
                ctx.fireChannelActive();
              }
            }
          }
      );
      ch.config().setAutoRead(false);
      assertFalse(ch.config().isAutoRead());

      assertTrue(ch.writeOutbound(Unpooled.EMPTY_BUFFER));
      assertTrue(readIssued);
      assertTrue(ch.finishAndReleaseAll());
    }
  }
}