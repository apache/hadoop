/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleHeader;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.mapred.ShuffleChannelHandler.shuffleHeaderToBytes;
import static org.apache.hadoop.mapred.ShuffleChannelInitializer.MAX_CONTENT_LENGTH;
import static org.apache.hadoop.mapred.ShuffleHandler.CONNECTION_CLOSE;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED;
import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT;
import static org.apache.hadoop.mapred.ShuffleHandler.TIMEOUT_HANDLER;
import static org.apache.hadoop.mapreduce.security.SecureShuffleUtils.HTTP_HEADER_URL_HASH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestShuffleChannelHandler extends TestShuffleHandlerBase {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TestShuffleChannelHandler.class);

  @Test
  public void testGetMapsFileRegion() throws IOException {
    final ShuffleTest t = createShuffleTest();
    final EmbeddedChannel shuffle = t.createShuffleHandlerChannelFileRegion();
    t.testGetAllAttemptsForReduce0NoKeepAlive(shuffle.outboundMessages(), shuffle);
  }

  @Test
  public void testGetMapsChunkedFileSSl() throws Exception {
    final ShuffleTest t = createShuffleTest();
    final LinkedList<Object> unencryptedMessages = new LinkedList<>();
    final EmbeddedChannel shuffle = t.createShuffleHandlerSSL(unencryptedMessages);
    t.testGetAllAttemptsForReduce0NoKeepAlive(unencryptedMessages, shuffle);
  }

  @Test
  public void testKeepAlive() throws Exception {
    // TODO: problems with keep-alive
    // current behaviour:
    //  a) mapreduce.shuffle.connection-keep-alive.enable=false
    //     + client request with &keepAlive=true
    //     ==> connection is kept
    //  b) mapreduce.shuffle.connection-keep-alive.enable=true
    //     ==> connection is kept
    //
    // a) seems like a bug
    // b) might be ok, because it's the default in HTTP/1.1
    Configuration conf = new Configuration();
    conf.set(SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, "false");
    conf.set(SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, "15");
    final ShuffleTest t = createShuffleTest(conf);
    final EmbeddedChannel shuffle = t.createShuffleHandlerChannelFileRegion();
    t.testKeepAlive(shuffle.outboundMessages(), shuffle);
  }

  @Test
  public void testKeepAliveSSL() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, "false");
    conf.set(SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, "15");
    final ShuffleTest t = createShuffleTest(conf);
    final LinkedList<Object> unencryptedMessages = new LinkedList<>();
    final EmbeddedChannel shuffle = t.createShuffleHandlerSSL(unencryptedMessages);
    t.testKeepAlive(unencryptedMessages, shuffle);
  }

  @Test
  public void tetKeepAliveTimeout() throws InterruptedException, IOException {
    Configuration conf = new Configuration();
    conf.set(SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, "true");
    conf.set(SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, "1");
    final ShuffleTest t = createShuffleTest(conf);
    final EmbeddedChannel shuffle = t.createShuffleHandlerChannelFileRegion();

    FullHttpRequest req = t.createRequest(getUri(TEST_JOB_ID, 0,
        Collections.singletonList(TEST_ATTEMPT_1), true));
    shuffle.writeInbound(req);
    t.assertResponse(shuffle.outboundMessages(),
        t.getExpectedHttpResponse(req, true, 46),
        t.getAttemptData(new Attempt(TEST_ATTEMPT_1, TEST_DATA_A))
    );
    assertTrue("keep-alive", shuffle.isActive());

    TimeUnit.SECONDS.sleep(3);
    shuffle.runScheduledPendingTasks();

    assertFalse("closed", shuffle.isActive());
  }

  @Test
  public void testIncompatibleShuffleVersion() {
    Configuration conf = new Configuration();
    conf.set(SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, "true");
    final ShuffleTest t = createShuffleTest(conf);
    final EmbeddedChannel shuffle = t.createShuffleHandlerChannelFileRegion();
    FullHttpRequest req = t.createRequest(getUri(TEST_JOB_ID, 0,
        Collections.singletonList(TEST_ATTEMPT_1), true));
    req.headers().set(ShuffleHeader.HTTP_HEADER_NAME, "invalid");
    shuffle.writeInbound(req);

    final EmbeddedChannel decoder = t.createHttpResponseChannel();
    for (Object obj : shuffle.outboundMessages()) {
      decoder.writeInbound(obj);
    }
    DefaultHttpResponse actual = decoder.readInbound();
    assertFalse(actual.headers().get(CONTENT_LENGTH).isEmpty());
    actual.headers().set(CONTENT_LENGTH, 0);

    assertEquals(getExpectedHttpResponse(HttpResponseStatus.BAD_REQUEST).toString(),
        actual.toString());

    assertFalse("closed", shuffle.isActive()); // known-issue
  }

  @Test
  public void testInvalidMapNoIndexFile() {
    final ShuffleTest t = createShuffleTest();
    final EmbeddedChannel shuffle = t.createShuffleHandlerChannelFileRegion();
    FullHttpRequest req = t.createRequest(getUri(TEST_JOB_ID, 0,
        Arrays.asList(TEST_ATTEMPT_1, "non-existing"), true));
    shuffle.writeInbound(req);

    final EmbeddedChannel decoder = t.createHttpResponseChannel();
    for (Object obj : shuffle.outboundMessages()) {
      decoder.writeInbound(obj);
    }

    DefaultHttpResponse actual = decoder.readInbound();
    assertFalse(actual.headers().get(CONTENT_LENGTH).isEmpty());
    actual.headers().set(CONTENT_LENGTH, 0);

    assertEquals(getExpectedHttpResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR).toString(),
        actual.toString());

    assertFalse("closed", shuffle.isActive());
  }

  @Test
  public void testInvalidMapNoDataFile() {
    final ShuffleTest t = createShuffleTest();
    final EmbeddedChannel shuffle = t.createShuffleHandlerChannelFileRegion();

    String dataFile = getDataFile(TEST_USER, tempDir.toAbsolutePath().toString(), TEST_ATTEMPT_2);
    assertTrue("should delete", new File(dataFile).delete());

    FullHttpRequest req = t.createRequest(getUri(TEST_JOB_ID, 0,
        Arrays.asList(TEST_ATTEMPT_1, TEST_ATTEMPT_2), false));
    shuffle.writeInbound(req);

    final EmbeddedChannel decoder = t.createHttpResponseChannel();
    for (Object obj : shuffle.outboundMessages()) {
      decoder.writeInbound(obj);
    }

    DefaultHttpResponse actual = decoder.readInbound();
    assertFalse(actual.headers().get(CONTENT_LENGTH).isEmpty());
    actual.headers().set(CONTENT_LENGTH, 0);

    assertEquals(getExpectedHttpResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR).toString(),
        actual.toString());

    assertFalse("closed", shuffle.isActive());
  }

  private DefaultHttpResponse getExpectedHttpResponse(HttpResponseStatus status) {
    DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
    response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
    response.headers().set(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    response.headers().set(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    response.headers().set(CONTENT_LENGTH, 0);
    return response;
  }

  private ShuffleTest createShuffleTest() {
    return createShuffleTest(new Configuration());
  }

  private ShuffleTest createShuffleTest(Configuration conf) {
    return new ShuffleTest(conf);
  }

  private File getResourceFile(String resourceName) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  static class Attempt {
    final String id;
    final String content;

    Attempt(String attempt, String content) {
      this.id = attempt;
      this.content = content;
    }
  }

  private class ShuffleTest {
    private final ShuffleChannelHandlerContext ctx;
    private final SecretKey shuffleSecretKey;

    ShuffleTest(Configuration conf) {
      JobConf jobConf = new JobConf(conf);
      MetricsSystem ms = DefaultMetricsSystem.instance();
      this.ctx = new ShuffleChannelHandlerContext(conf,
          new ConcurrentHashMap<>(),
          new JobTokenSecretManager(),
          createLoadingCache(),
          new IndexCache(jobConf),
          ms.register(new ShuffleHandler.ShuffleMetrics()),
          new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
      );

      JobTokenIdentifier tokenId = new JobTokenIdentifier(new Text(TEST_JOB_ID));
      Token<JobTokenIdentifier> token = new Token<>(tokenId, ctx.secretManager);
      shuffleSecretKey = JobTokenSecretManager.createSecretKey(token.getPassword());

      ctx.userRsrc.put(TEST_JOB_ID, TEST_USER);
      ctx.secretManager.addTokenForJob(TEST_JOB_ID, token);
    }

    public FullHttpRequest createRequest(String uri) {
      FullHttpRequest request =
          new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
      request.headers().set(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      request.headers().set(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      request.headers().set(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      try {
        String msgToEncode = SecureShuffleUtils.buildMsgFrom(new URL("http", "", ctx.port, uri));
        request.headers().set(HTTP_HEADER_URL_HASH,
            SecureShuffleUtils.hashFromString(msgToEncode, shuffleSecretKey));
      } catch (IOException e) {
        e.printStackTrace();
        fail("Could not create URL hash for test request");
      }

      return request;
    }

    public DefaultHttpResponse getExpectedHttpResponse(
        FullHttpRequest request, boolean keepAlive, long contentLength) {
      DefaultHttpResponse response =
          new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      HttpHeaders headers = response.headers();
      try {
        SecretKey tokenSecret = ctx.secretManager.retrieveTokenSecret(TEST_JOB_ID);
        headers.set(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH,
            SecureShuffleUtils.generateHash(
                request.headers().get(HTTP_HEADER_URL_HASH).getBytes(Charsets.UTF_8),
                tokenSecret));
      } catch (SecretManager.InvalidToken e) {
        fail("Could not generate reply hash");
      }
      headers.set(ShuffleHeader.HTTP_HEADER_NAME, ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      headers.set(ShuffleHeader.HTTP_HEADER_VERSION, ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      if (keepAlive) {
        headers.set(HttpHeader.CONNECTION.asString(), HttpHeader.KEEP_ALIVE.asString());
        headers.set(HttpHeader.KEEP_ALIVE.asString(), "timeout=" + ctx.connectionKeepAliveTimeOut);
      } else {
        response.headers().set(HttpHeader.CONNECTION.asString(), CONNECTION_CLOSE);
      }
      HttpUtil.setContentLength(response, contentLength);
      return response;
    }

    private void testGetAllAttemptsForReduce0NoKeepAlive(
        java.util.Queue<Object> outboundMessages, EmbeddedChannel shuffle) throws IOException {
      final FullHttpRequest request = createRequest(
          getUri(TEST_JOB_ID, 0,
              Arrays.asList(TEST_ATTEMPT_1, TEST_ATTEMPT_2, TEST_ATTEMPT_3), false));
      shuffle.writeInbound(request);
      assertResponse(outboundMessages,
          getExpectedHttpResponse(request, false, 138),
          getAllAttemptsForReduce0()
      );
      assertFalse("no keep-alive", shuffle.isActive());
    }

    private void testKeepAlive(java.util.Queue<Object> messages,
                               EmbeddedChannel shuffle) throws IOException {
      final FullHttpRequest req1 = createRequest(
          getUri(TEST_JOB_ID, 0, Collections.singletonList(TEST_ATTEMPT_1), true));
      shuffle.writeInbound(req1);
      assertResponse(messages,
          getExpectedHttpResponse(req1, true, 46),
          getAttemptData(new Attempt(TEST_ATTEMPT_1, TEST_DATA_A))
      );
      assertTrue("keep-alive", shuffle.isActive());
      messages.clear();

      final FullHttpRequest req2 = createRequest(
          getUri(TEST_JOB_ID, 0, Collections.singletonList(TEST_ATTEMPT_2), true));
      shuffle.writeInbound(req2);
      assertResponse(messages,
          getExpectedHttpResponse(req2, true, 46),
          getAttemptData(new Attempt(TEST_ATTEMPT_2, TEST_DATA_B))
      );
      assertTrue("keep-alive", shuffle.isActive());
      messages.clear();

      final FullHttpRequest req3 = createRequest(
          getUri(TEST_JOB_ID, 0, Collections.singletonList(TEST_ATTEMPT_3), false));
      shuffle.writeInbound(req3);
      assertResponse(messages,
          getExpectedHttpResponse(req3, false, 46),
          getAttemptData(new Attempt(TEST_ATTEMPT_3, TEST_DATA_C))
      );
      assertFalse("no keep-alive", shuffle.isActive());
    }

    private ArrayList<ByteBuf> getAllAttemptsForReduce0() throws IOException {
      return getAttemptData(
          new Attempt(TEST_ATTEMPT_1, TEST_DATA_A),
          new Attempt(TEST_ATTEMPT_2, TEST_DATA_B),
          new Attempt(TEST_ATTEMPT_3, TEST_DATA_C)
      );
    }

    private ArrayList<ByteBuf> getAttemptData(Attempt... attempts) throws IOException {
      ArrayList<ByteBuf> data = new ArrayList<>();
      for (Attempt attempt : attempts) {
        data.add(shuffleHeaderToBytes(new ShuffleHeader(attempt.id, attempt.content.length(),
            attempt.content.length() * 2L, 0)));
        data.add(Unpooled.copiedBuffer(attempt.content.getBytes(StandardCharsets.UTF_8)));
      }
      return data;
    }

    private void assertResponse(java.util.Queue<Object> outboundMessages,
                                DefaultHttpResponse response,
                                List<ByteBuf> content) {
      final EmbeddedChannel decodeChannel = createHttpResponseChannel();

      content.add(LastHttpContent.EMPTY_LAST_CONTENT.content());

      int i = 0;
      for (Object outboundMessage : outboundMessages) {
        ByteBuf actualBytes = ((ByteBuf) outboundMessage);
        String actualHexdump = ByteBufUtil.prettyHexDump(actualBytes);
        LOG.info("\n{}", actualHexdump);

        decodeChannel.writeInbound(actualBytes);
        Object obj = decodeChannel.readInbound();
        LOG.info("Decoded object: {}", obj);

        if (i == 0) {
          DefaultHttpResponse resp = (DefaultHttpResponse) obj;
          assertEquals(response.toString(), resp.toString());
        }
        if (i > 0 && i <= content.size()) {
          assertEquals("data should match",
              ByteBufUtil.prettyHexDump(content.get(i - 1)), actualHexdump);
        }

        i++;
      }

      // This check is done after to have better debug logs on failure.
      assertEquals("all data should match", content.size() + 1, outboundMessages.size());
    }

    public EmbeddedChannel createShuffleHandlerChannelFileRegion() {
      final EmbeddedChannel channel = createShuffleHandlerChannel();

      channel.pipeline().addFirst(
          new MessageToMessageEncoder<FileRegion>() {
            @Override
            protected void encode(
                ChannelHandlerContext cCtx, FileRegion msg, List<Object> out) throws Exception {
              ByteArrayOutputStream stream = new ByteArrayOutputStream();
              WritableByteChannel wbc = Channels.newChannel(stream);
              msg.transferTo(wbc, msg.position());
              out.add(Unpooled.wrappedBuffer(stream.toByteArray()));
            }
          }
      );

      return channel;
    }

    public EmbeddedChannel createSSLClient() throws Exception {
      final EmbeddedChannel channel = createShuffleHandlerChannel();

      SSLContext sc = SSLContext.getInstance("SSL");

      final TrustManager trm = new X509TrustManager() {
        public X509Certificate[] getAcceptedIssuers() {
          return null;
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType) {
        }

        public void checkServerTrusted(X509Certificate[] certs, String authType) {
        }
      };

      sc.init(null, new TrustManager[]{trm}, null);

      final SSLEngine sslEngine = sc.createSSLEngine();
      sslEngine.setUseClientMode(true);
      channel.pipeline().addFirst("ssl", new SslHandler(sslEngine));

      return channel;
    }

    public EmbeddedChannel createShuffleHandlerSSL(java.util.Queue<Object> unencryptedMessages)
        throws Exception {
      final EmbeddedChannel channel = createShuffleHandlerChannel();
      // SelfSignedCertificate was generated manually with:
      //  openssl req -x509 -newkey rsa:4096 -keyout key.pem \
      //    -out cert.pem -sha256 -days 3650 -nodes -subj '/CN=localhost'
      // Because:
      //  SelfSignedCertificate ssc = new SelfSignedCertificate();
      // Throws: Failed to generate a self-signed X.509 certificate using Bouncy Castle
      final SslContext sslCtx = SslContextBuilder
          .forServer(getResourceFile("cert.pem"), getResourceFile("key.pem"))
          .build();
      final SslHandler sslHandler = sslCtx.newHandler(ByteBufAllocator.DEFAULT);
      channel.pipeline().addFirst("ssl", sslHandler);

      channel.pipeline().addAfter("ssl", "unencrypted", new MessageToMessageEncoder<ByteBuf>() {
        @Override
        protected void encode(ChannelHandlerContext cCtx, ByteBuf msg, List<Object> out) {
          unencryptedMessages.add(msg.copy());
          out.add(msg.retain());
        }
      });

      channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
        @Override
        public void userEventTriggered(ChannelHandlerContext cCtx, Object evt) {
          LOG.info("EVENT: {}", evt);
        }
      });

      // SSLHandshake must be done, otherwise messages are buffered
      final EmbeddedChannel client = createSSLClient();
      for (Object obj : client.outboundMessages()) {
        channel.writeInbound(obj);
      }
      client.outboundMessages().clear();
      for (Object obj : channel.outboundMessages()) {
        client.writeInbound(obj);
      }
      channel.outboundMessages().clear();
      for (Object obj : client.outboundMessages()) {
        channel.writeInbound(obj);
      }
      client.outboundMessages().clear();

      return channel;
    }

    public EmbeddedChannel createShuffleHandlerChannel() {
      final EmbeddedChannel channel = new EmbeddedChannel();
      channel.pipeline().addLast("http", new HttpServerCodec());
      channel.pipeline().addLast("aggregator", new HttpObjectAggregator(MAX_CONTENT_LENGTH));
      channel.pipeline().addLast("chunking", new ChunkedWriteHandler());
      channel.pipeline().addLast("shuffle", new ShuffleChannelHandler(ctx));
      channel.pipeline().addLast(TIMEOUT_HANDLER,
          new ShuffleHandler.TimeoutHandler(ctx.connectionKeepAliveTimeOut));
      return channel;
    }

    public EmbeddedChannel createHttpResponseChannel() {
      return new EmbeddedChannel(
          new HttpResponseDecoder()
      );
    }
  }
}
