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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.DefaultFileRegion;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.hadoop.test.GenericTestUtils;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.URL;
import java.net.SocketAddress;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.task.reduce.ShuffleHeader;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryLocalPathHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.records.Version;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestShuffleHandler {
  static final long MiB = 1024 * 1024; 
  private static final Logger LOG =
      LoggerFactory.getLogger(TestShuffleHandler.class);
  private static final File ABS_LOG_DIR = GenericTestUtils.getTestDir(
      TestShuffleHandler.class.getSimpleName() + "LocDir");
  private static final long ATTEMPT_ID = 12345L;
  private static final long ATTEMPT_ID_2 = 12346L;
  private static final HttpResponseStatus OK_STATUS = new HttpResponseStatus(200, "OK");


  //Control test execution properties with these flags
  private static final boolean DEBUG_MODE = false;
  //WARNING: If this is set to true and proxy server is not running, tests will fail!
  private static final boolean USE_PROXY = false;
  private static final int HEADER_WRITE_COUNT = 100000;
  private static final int ARBITRARY_NEGATIVE_TIMEOUT_SECONDS = -100;
  private static TestExecution TEST_EXECUTION;

  private static class TestExecution {
    private static final int DEFAULT_KEEP_ALIVE_TIMEOUT_SECONDS = 1;
    private static final int DEBUG_KEEP_ALIVE_SECONDS = 1000;
    private static final int DEFAULT_PORT = 0; //random port
    private static final int FIXED_PORT = 8088;
    private static final String PROXY_HOST = "127.0.0.1";
    private static final int PROXY_PORT = 8888;
    private static final int CONNECTION_DEBUG_TIMEOUT = 1000000;
    private final boolean debugMode;
    private final boolean useProxy;

    TestExecution(boolean debugMode, boolean useProxy) {
      this.debugMode = debugMode;
      this.useProxy = useProxy;
    }

    int getKeepAliveTimeout() {
      if (debugMode) {
        return DEBUG_KEEP_ALIVE_SECONDS;
      }
      return DEFAULT_KEEP_ALIVE_TIMEOUT_SECONDS;
    }

    HttpURLConnection openConnection(URL url) throws IOException {
      HttpURLConnection conn;
      if (useProxy) {
        Proxy proxy
            = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(PROXY_HOST, PROXY_PORT));
        conn = (HttpURLConnection) url.openConnection(proxy);
      } else {
        conn = (HttpURLConnection) url.openConnection();
      }
      return conn;
    }

    int shuffleHandlerPort() {
      if (debugMode) {
        return FIXED_PORT;
      } else {
        return DEFAULT_PORT;
      }
    }

    void parameterizeConnection(URLConnection conn) {
      if (DEBUG_MODE) {
        conn.setReadTimeout(CONNECTION_DEBUG_TIMEOUT);
        conn.setConnectTimeout(CONNECTION_DEBUG_TIMEOUT);
      }
    }
  }

  private static class ResponseConfig {
    private final int headerWriteCount;
    private final int mapOutputCount;
    private final int contentLengthOfOneMapOutput;
    private long headerSize;
    public long contentLengthOfResponse;

    ResponseConfig(int headerWriteCount, int mapOutputCount,
        int contentLengthOfOneMapOutput) {
      if (mapOutputCount <= 0 && contentLengthOfOneMapOutput > 0) {
        throw new IllegalStateException("mapOutputCount should be at least 1");
      }
      this.headerWriteCount = headerWriteCount;
      this.mapOutputCount = mapOutputCount;
      this.contentLengthOfOneMapOutput = contentLengthOfOneMapOutput;
    }

    private void setHeaderSize(long headerSize) {
      this.headerSize = headerSize;
      long contentLengthOfAllHeaders = headerWriteCount * headerSize;
      this.contentLengthOfResponse = computeContentLengthOfResponse(contentLengthOfAllHeaders);
      LOG.debug("Content-length of all headers: {}", contentLengthOfAllHeaders);
      LOG.debug("Content-length of one MapOutput: {}", contentLengthOfOneMapOutput);
      LOG.debug("Content-length of final HTTP response: {}", contentLengthOfResponse);
    }

    private long computeContentLengthOfResponse(long contentLengthOfAllHeaders) {
      int mapOutputCountMultiplier = mapOutputCount;
      if (mapOutputCount == 0) {
        mapOutputCountMultiplier = 1;
      }
      return (contentLengthOfAllHeaders + contentLengthOfOneMapOutput) * mapOutputCountMultiplier;
    }
  }

  private enum ShuffleUrlType {
    SIMPLE, WITH_KEEPALIVE, WITH_KEEPALIVE_MULTIPLE_MAP_IDS, WITH_KEEPALIVE_NO_MAP_IDS
  }

  private static class InputStreamReadResult {
    final String asString;
    int totalBytesRead;

    InputStreamReadResult(byte[] bytes, int totalBytesRead) {
      this.asString = new String(bytes, StandardCharsets.UTF_8);
      this.totalBytesRead = totalBytesRead;
    }
  }

  private static abstract class AdditionalMapOutputSenderOperations {
    public abstract ChannelFuture perform(ChannelHandlerContext ctx, Channel ch) throws IOException;
  }

  private class ShuffleHandlerForKeepAliveTests extends ShuffleHandler {
    final LastSocketAddress lastSocketAddress = new LastSocketAddress();
    final ArrayList<Throwable> failures = new ArrayList<>();
    final ShuffleHeaderProvider shuffleHeaderProvider;
    final HeaderPopulator headerPopulator;
    MapOutputSender mapOutputSender;
    private Consumer<IdleStateEvent> channelIdleCallback;
    private CustomTimeoutHandler customTimeoutHandler;
    private boolean failImmediatelyOnErrors = false;
    private boolean closeChannelOnError = true;
    private ResponseConfig responseConfig;

    ShuffleHandlerForKeepAliveTests(long attemptId, ResponseConfig responseConfig,
        Consumer<IdleStateEvent> channelIdleCallback) throws IOException {
      this(attemptId, responseConfig);
      this.channelIdleCallback = channelIdleCallback;
    }

    ShuffleHandlerForKeepAliveTests(long attemptId, ResponseConfig responseConfig)
        throws IOException {
      this.responseConfig = responseConfig;
      this.shuffleHeaderProvider = new ShuffleHeaderProvider(attemptId);
      this.responseConfig.setHeaderSize(shuffleHeaderProvider.getShuffleHeaderSize());
      this.headerPopulator = new HeaderPopulator(this, responseConfig, shuffleHeaderProvider, true);
      this.mapOutputSender = new MapOutputSender(responseConfig, lastSocketAddress,
          shuffleHeaderProvider);
      setUseOutboundExceptionHandler(true);
    }

    public void setFailImmediatelyOnErrors(boolean failImmediatelyOnErrors) {
      this.failImmediatelyOnErrors = failImmediatelyOnErrors;
    }

    public void setCloseChannelOnError(boolean closeChannelOnError) {
      this.closeChannelOnError = closeChannelOnError;
    }

    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      // replace the shuffle handler with one stubbed for testing
      return new Shuffle(conf) {
        @Override
        protected MapOutputInfo getMapOutputInfo(String mapId, int reduce,
            String jobId, String user) {
          return null;
        }
        @Override
        protected void verifyRequest(String appid, ChannelHandlerContext ctx,
            HttpRequest request, HttpResponse response, URL requestUri) {
        }

        @Override
        protected void populateHeaders(List<String> mapIds, String jobId,
            String user, int reduce, HttpRequest request,
            HttpResponse response, boolean keepAliveParam,
            Map<String, MapOutputInfo> infoMap) throws IOException {
          long contentLength = headerPopulator.populateHeaders(
              keepAliveParam);
          super.setResponseHeaders(response, keepAliveParam, contentLength);
        }

        @Override
        protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
            Channel ch, String user, String mapId, int reduce,
            MapOutputInfo info) throws IOException {
          return mapOutputSender.send(ctx, ch);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
          ctx.pipeline().replace(HttpResponseEncoder.class, ENCODER_HANDLER_NAME,
              new LoggingHttpResponseEncoder(false));
          replaceTimeoutHandlerWithCustom(ctx);
          LOG.debug("Modified pipeline: {}", ctx.pipeline());
          super.channelActive(ctx);
        }

        private void replaceTimeoutHandlerWithCustom(ChannelHandlerContext ctx) {
          TimeoutHandler oldTimeoutHandler =
              (TimeoutHandler)ctx.pipeline().get(TIMEOUT_HANDLER);
          int timeoutValue =
              oldTimeoutHandler.getConnectionKeepAliveTimeOut();
          customTimeoutHandler = new CustomTimeoutHandler(timeoutValue, channelIdleCallback);
          ctx.pipeline().replace(TIMEOUT_HANDLER, TIMEOUT_HANDLER, customTimeoutHandler);
        }

        @Override
        protected void sendError(ChannelHandlerContext ctx,
            HttpResponseStatus status) {
          String message = "Error while processing request. Status: " + status;
          handleError(ctx, message);
          if (failImmediatelyOnErrors) {
            stop();
          }
        }

        @Override
        protected void sendError(ChannelHandlerContext ctx, String message,
            HttpResponseStatus status) {
          String errMessage = String.format("Error while processing request. " +
              "Status: " +
              "%s, message: %s", status, message);
          handleError(ctx, errMessage);
          if (failImmediatelyOnErrors) {
            stop();
          }
        }
      };
    }

    private void handleError(ChannelHandlerContext ctx, String message) {
      LOG.error(message);
      failures.add(new Error(message));
      if (closeChannelOnError) {
        LOG.warn("sendError: Closing channel");
        ctx.channel().close();
      }
    }

    private class CustomTimeoutHandler extends TimeoutHandler {
      private boolean channelIdle = false;
      private final Consumer<IdleStateEvent> channelIdleCallback;

      CustomTimeoutHandler(int connectionKeepAliveTimeOut,
          Consumer<IdleStateEvent> channelIdleCallback) {
        super(connectionKeepAliveTimeOut);
        this.channelIdleCallback = channelIdleCallback;
      }

      @Override
      public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) {
        LOG.debug("Channel idle");
        this.channelIdle = true;
        if (channelIdleCallback != null) {
          LOG.debug("Calling channel idle callback..");
          channelIdleCallback.accept(e);
        }
        super.channelIdle(ctx, e);
      }
    }
  }

  private static class MapOutputSender {
    private final ResponseConfig responseConfig;
    private final LastSocketAddress lastSocketAddress;
    private final ShuffleHeaderProvider shuffleHeaderProvider;
    private AdditionalMapOutputSenderOperations additionalMapOutputSenderOperations;

    MapOutputSender(ResponseConfig responseConfig, LastSocketAddress lastSocketAddress,
        ShuffleHeaderProvider shuffleHeaderProvider) {
      this.responseConfig = responseConfig;
      this.lastSocketAddress = lastSocketAddress;
      this.shuffleHeaderProvider = shuffleHeaderProvider;
    }

    public ChannelFuture send(ChannelHandlerContext ctx, Channel ch) throws IOException {
      LOG.debug("In MapOutputSender#send");
      lastSocketAddress.setAddress(ch.remoteAddress());
      ShuffleHeader header = shuffleHeaderProvider.createNewShuffleHeader();
      ChannelFuture future = writeHeaderNTimes(ch, header, responseConfig.headerWriteCount);
      // This is the last operation
      // It's safe to increment ShuffleHeader counter for better identification
      shuffleHeaderProvider.incrementCounter();
      if (additionalMapOutputSenderOperations != null) {
        return additionalMapOutputSenderOperations.perform(ctx, ch);
      }
      return future;
    }

    private ChannelFuture writeHeaderNTimes(Channel ch, ShuffleHeader header, int iterations)
        throws IOException {
      DataOutputBuffer dob = new DataOutputBuffer();
      for (int i = 0; i < iterations; ++i) {
        header.write(dob);
      }
      LOG.debug("MapOutputSender#writeHeaderNTimes WriteAndFlush big chunk of data, " +
          "outputBufferSize: " + dob.size());
      return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
    }
  }

  private static class ShuffleHeaderProvider {
    private final long attemptId;
    private int attemptCounter = 0;
    private int cachedSize = Integer.MIN_VALUE;

    ShuffleHeaderProvider(long attemptId) {
      this.attemptId = attemptId;
    }

    ShuffleHeader createNewShuffleHeader() {
      return new ShuffleHeader(String.format("attempt_%s_1_m_1_0%s", attemptId, attemptCounter),
          5678, 5678, 1);
    }

    void incrementCounter() {
      attemptCounter++;
    }

    private int getShuffleHeaderSize() throws IOException {
      if (cachedSize != Integer.MIN_VALUE) {
        return cachedSize;
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      ShuffleHeader header = createNewShuffleHeader();
      header.write(dob);
      cachedSize = dob.size();
      return cachedSize;
    }
  }

  private static class HeaderPopulator {
    private final ShuffleHandler shuffleHandler;
    private final boolean disableKeepAliveConfig;
    private final ShuffleHeaderProvider shuffleHeaderProvider;
    private final ResponseConfig responseConfig;

    HeaderPopulator(ShuffleHandler shuffleHandler,
        ResponseConfig responseConfig,
        ShuffleHeaderProvider shuffleHeaderProvider,
        boolean disableKeepAliveConfig) {
      this.shuffleHandler = shuffleHandler;
      this.responseConfig = responseConfig;
      this.disableKeepAliveConfig = disableKeepAliveConfig;
      this.shuffleHeaderProvider = shuffleHeaderProvider;
    }

    public long populateHeaders(boolean keepAliveParam) throws IOException {
      // Send some dummy data (populate content length details)
      DataOutputBuffer dob = new DataOutputBuffer();
      for (int i = 0; i < responseConfig.headerWriteCount; ++i) {
        ShuffleHeader header =
            shuffleHeaderProvider.createNewShuffleHeader();
        header.write(dob);
      }
      // for testing purpose;
      // disable connectionKeepAliveEnabled if keepAliveParam is available
      if (keepAliveParam && disableKeepAliveConfig) {
        shuffleHandler.connectionKeepAliveEnabled = false;
      }
      return responseConfig.contentLengthOfResponse;
    }
  }

  private static final class HttpConnectionData {
    private final Map<String, List<String>> headers;
    private HttpURLConnection conn;
    private final int payloadLength;
    private final SocketAddress socket;
    private int responseCode = -1;

    private HttpConnectionData(HttpURLConnection conn, int payloadLength,
        SocketAddress socket) {
      this.headers = conn.getHeaderFields();
      this.conn = conn;
      this.payloadLength = payloadLength;
      this.socket = socket;
      try {
        this.responseCode = conn.getResponseCode();
      } catch (IOException e) {
        fail("Failed to read response code from connection: " + conn);
      }
    }

    static HttpConnectionData create(HttpURLConnection conn, int payloadLength,
        SocketAddress socket) {
      return new HttpConnectionData(conn, payloadLength, socket);
    }
  }

  private static final class HttpConnectionAssert {
    private final HttpConnectionData connData;

    private HttpConnectionAssert(HttpConnectionData connData) {
      this.connData = connData;
    }

    static HttpConnectionAssert create(HttpConnectionData connData) {
      return new HttpConnectionAssert(connData);
    }

    public static void assertKeepAliveConnectionsAreSame(
        HttpConnectionHelper httpConnectionHelper) {
      assertTrue("At least two connection data " +
          "is required to perform this assertion",
          httpConnectionHelper.connectionData.size() >= 2);
      SocketAddress firstAddress = httpConnectionHelper.getConnectionData(0).socket;
      SocketAddress secondAddress = httpConnectionHelper.getConnectionData(1).socket;
      Assert.assertNotNull("Initial shuffle address should not be null",
          firstAddress);
      Assert.assertNotNull("Keep-Alive shuffle address should not be null",
          secondAddress);
      assertEquals("Initial shuffle address and keep-alive shuffle "
          + "address should be the same", firstAddress, secondAddress);
    }

    public HttpConnectionAssert expectKeepAliveWithTimeout(long timeout) {
      assertEquals(HttpURLConnection.HTTP_OK, connData.responseCode);
      assertHeaderValue(HttpHeader.CONNECTION, HttpHeader.KEEP_ALIVE.asString());
      assertHeaderValue(HttpHeader.KEEP_ALIVE, "timeout=" + timeout);
      return this;
    }

    public HttpConnectionAssert expectBadRequest(long timeout) {
      assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, connData.responseCode);
      assertHeaderValue(HttpHeader.CONNECTION, HttpHeader.KEEP_ALIVE.asString());
      assertHeaderValue(HttpHeader.KEEP_ALIVE, "timeout=" + timeout);
      return this;
    }

    public HttpConnectionAssert expectResponseContentLength(long size) {
      assertEquals(size, connData.payloadLength);
      return this;
    }

    private void assertHeaderValue(HttpHeader header, String expectedValue) {
      List<String> headerList = connData.headers.get(header.asString());
      Assert.assertNotNull("Got null header value for header: " + header, headerList);
      Assert.assertFalse("Got empty header value for header: " + header, headerList.isEmpty());
      assertEquals("Unexpected size of header list for header: " + header, 1,
          headerList.size());
      assertEquals(expectedValue, headerList.get(0));
    }
  }

  private static class HttpConnectionHelper {
    private final LastSocketAddress lastSocketAddress;
    List<HttpConnectionData> connectionData = new ArrayList<>();

    HttpConnectionHelper(LastSocketAddress lastSocketAddress) {
      this.lastSocketAddress = lastSocketAddress;
    }

    public void connectToUrls(String[] urls, ResponseConfig responseConfig) throws IOException {
      connectToUrlsInternal(urls, responseConfig, HttpURLConnection.HTTP_OK);
    }

    public void connectToUrls(String[] urls, ResponseConfig responseConfig, int expectedHttpStatus)
        throws IOException {
      connectToUrlsInternal(urls, responseConfig, expectedHttpStatus);
    }

    private void connectToUrlsInternal(String[] urls, ResponseConfig responseConfig,
        int expectedHttpStatus) throws IOException {
      int requests = urls.length;
      int expectedConnections = urls.length;
      LOG.debug("Will connect to URLs: {}", Arrays.toString(urls));
      for (int reqIdx = 0; reqIdx < requests; reqIdx++) {
        String urlString = urls[reqIdx];
        LOG.debug("Connecting to URL: {}", urlString);
        URL url = new URL(urlString);
        HttpURLConnection conn = TEST_EXECUTION.openConnection(url);
        conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
            ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
        conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
            ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
        TEST_EXECUTION.parameterizeConnection(conn);
        conn.connect();
        if (expectedHttpStatus == HttpURLConnection.HTTP_BAD_REQUEST) {
          //Catch exception as error are caught with overridden sendError method
          //Caught errors will be validated later.
          try {
            DataInputStream input = new DataInputStream(conn.getInputStream());
          } catch (Exception e) {
            expectedConnections--;
            continue;
          }
        }
        DataInputStream input = new DataInputStream(conn.getInputStream());
        LOG.debug("Opened DataInputStream for connection: {}/{}", (reqIdx + 1), requests);
        ShuffleHeader header = new ShuffleHeader();
        header.readFields(input);
        InputStreamReadResult result = readDataFromInputStream(input);
        result.totalBytesRead += responseConfig.headerSize;
        int expectedContentLength =
            Integer.parseInt(conn.getHeaderField(HttpHeader.CONTENT_LENGTH.asString()));

        if (result.totalBytesRead != expectedContentLength) {
          throw new IOException(String.format("Premature EOF InputStream. " +
              "Expected content-length: %s, " +
              "Actual content-length: %s", expectedContentLength, result.totalBytesRead));
        }
        connectionData.add(HttpConnectionData
            .create(conn, result.totalBytesRead, lastSocketAddress.getSocketAddres()));
        input.close();
        LOG.debug("Finished all interactions with URL: {}. Progress: {}/{}", url, (reqIdx + 1),
            requests);
      }
      assertEquals(expectedConnections, connectionData.size());
    }

    void validate(Consumer<HttpConnectionData> connDataValidator) {
      for (int i = 0; i < connectionData.size(); i++) {
        LOG.debug("Validating connection data #{}", (i + 1));
        HttpConnectionData connData = connectionData.get(i);
        connDataValidator.accept(connData);
      }
    }

    HttpConnectionData getConnectionData(int i) {
      return connectionData.get(i);
    }

    private static InputStreamReadResult readDataFromInputStream(
        InputStream input) throws IOException {
      ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int bytesRead;
      int totalBytesRead = 0;
      while ((bytesRead = input.read(buffer)) != -1) {
        dataStream.write(buffer, 0, bytesRead);
        totalBytesRead += bytesRead;
      }
      LOG.debug("Read total bytes: " + totalBytesRead);
      dataStream.flush();
      return new InputStreamReadResult(dataStream.toByteArray(), totalBytesRead);
    }
  }

  class ShuffleHandlerForTests extends ShuffleHandler {
    public final ArrayList<Throwable> failures = new ArrayList<>();

    ShuffleHandlerForTests() {
      setUseOutboundExceptionHandler(true);
    }

    ShuffleHandlerForTests(MetricsSystem ms) {
      super(ms);
      setUseOutboundExceptionHandler(true);
    }

    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      return new Shuffle(conf) {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx,
            Throwable cause) throws Exception {
          LOG.debug("ExceptionCaught");
          failures.add(cause);
          super.exceptionCaught(ctx, cause);
        }
      };
    }
  }

  class MockShuffleHandler extends org.apache.hadoop.mapred.ShuffleHandler {
    final ArrayList<Throwable> failures = new ArrayList<>();

    private final AuxiliaryLocalPathHandler pathHandler =
        new TestAuxiliaryLocalPathHandler();

    MockShuffleHandler() {
      setUseOutboundExceptionHandler(true);
    }

    MockShuffleHandler(MetricsSystem ms) {
      super(ms);
      setUseOutboundExceptionHandler(true);
    }

    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      return new Shuffle(conf) {
        @Override
        protected void verifyRequest(String appid, ChannelHandlerContext ctx,
            HttpRequest request, HttpResponse response, URL requestUri)
            throws IOException {
        }
        @Override
        protected MapOutputInfo getMapOutputInfo(String mapId, int reduce,
            String jobId, String user) {
          // Do nothing.
          return null;
        }
        @Override
        protected void populateHeaders(List<String> mapIds, String jobId,
            String user, int reduce, HttpRequest request,
            HttpResponse response, boolean keepAliveParam,
            Map<String, MapOutputInfo> infoMap) {
          // Do nothing.
        }
        @Override
        protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
            Channel ch, String user, String mapId, int reduce,
            MapOutputInfo info) throws IOException {

          ShuffleHeader header =
              new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
          DataOutputBuffer dob = new DataOutputBuffer();
          header.write(dob);
          ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          dob = new DataOutputBuffer();
          for (int i = 0; i < 100; ++i) {
            header.write(dob);
          }
          return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx,
            Throwable cause) throws Exception {
          LOG.debug("ExceptionCaught");
          failures.add(cause);
          super.exceptionCaught(ctx, cause);
        }
      };
    }

    @Override
    public AuxiliaryLocalPathHandler getAuxiliaryLocalPathHandler() {
      return pathHandler;
    }
  }

  private class TestAuxiliaryLocalPathHandler
      implements AuxiliaryLocalPathHandler {
    @Override
    public Path getLocalPathForRead(String path) {
      return new Path(ABS_LOG_DIR.getAbsolutePath(), path);
    }

    @Override
    public Path getLocalPathForWrite(String path) {
      return new Path(ABS_LOG_DIR.getAbsolutePath());
    }

    @Override
    public Path getLocalPathForWrite(String path, long size) {
      return new Path(ABS_LOG_DIR.getAbsolutePath());
    }

    @Override
    public Iterable<Path> getAllLocalPathsForRead(String path) {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(ABS_LOG_DIR.getAbsolutePath()));
      return paths;
    }
  }

  private static class MockShuffleHandler2 extends
      org.apache.hadoop.mapred.ShuffleHandler {
    final ArrayList<Throwable> failures = new ArrayList<>(1);
    boolean socketKeepAlive = false;

    MockShuffleHandler2() {
      setUseOutboundExceptionHandler(true);
    }

    MockShuffleHandler2(MetricsSystem ms) {
      super(ms);
      setUseOutboundExceptionHandler(true);
    }

    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      return new Shuffle(conf) {
        @Override
        protected void verifyRequest(String appid, ChannelHandlerContext ctx,
            HttpRequest request, HttpResponse response, URL requestUri) {
          SocketChannel channel = (SocketChannel)(ctx.channel());
          socketKeepAlive = channel.config().isKeepAlive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx,
            Throwable cause) throws Exception {
          LOG.debug("ExceptionCaught");
          failures.add(cause);
          super.exceptionCaught(ctx, cause);
        }
      };
    }

    protected boolean isSocketKeepAlive() {
      return socketKeepAlive;
    }
  }

  @Rule
  public TestName name = new TestName();

  @Before
  public void setup() {
    TEST_EXECUTION = new TestExecution(DEBUG_MODE, USE_PROXY);
  }

  @After
  public void tearDown() {
    int port = TEST_EXECUTION.shuffleHandlerPort();
    if (isPortUsed(port)) {
      String msg = String.format("Port is being used: %d. " +
          "Current testcase name: %s",
          port, name.getMethodName());
      throw new IllegalStateException(msg);
    }
  }

  private static boolean isPortUsed(int port) {
    if (port == 0) {
      //Don't check if port is 0
      return false;
    }
    try (Socket ignored = new Socket("localhost", port)) {
      return true;
    } catch (IOException e) {
      LOG.error("Port: {}, port check result: {}", port, e.getMessage());
      return false;
    }
  }

  /**
   * Test the validation of ShuffleHandler's meta-data's serialization and
   * de-serialization.
   *
   * @throws Exception exception
   */
  @Test (timeout = 10000)
  public void testSerializeMeta()  throws Exception {
    assertEquals(1, ShuffleHandler.deserializeMetaData(
        ShuffleHandler.serializeMetaData(1)));
    assertEquals(-1, ShuffleHandler.deserializeMetaData(
        ShuffleHandler.serializeMetaData(-1)));
    assertEquals(8080, ShuffleHandler.deserializeMetaData(
        ShuffleHandler.serializeMetaData(8080)));
  }

  /**
   * Validate shuffle connection and input/output metrics.
   *
   * @throws Exception exception
   */
  @Test (timeout = 10000)
  public void testShuffleMetrics() throws Exception {
    MetricsSystem ms = new MetricsSystemImpl();
    ShuffleHandler sh = new ShuffleHandlerForTests(ms);
    ChannelFuture cf = mock(ChannelFuture.class);
    when(cf.isSuccess()).thenReturn(true).thenReturn(false);

    sh.metrics.shuffleConnections.incr();
    sh.metrics.shuffleOutputBytes.incr(MiB);
    sh.metrics.shuffleConnections.incr();
    sh.metrics.shuffleOutputBytes.incr(2*MiB);

    checkShuffleMetrics(ms, 3*MiB, 0, 0, 2);

    sh.metrics.operationComplete(cf);
    sh.metrics.operationComplete(cf);

    checkShuffleMetrics(ms, 3*MiB, 1, 1, 0);

    sh.stop();
  }

  static void checkShuffleMetrics(MetricsSystem ms, long bytes, int failed,
                                  int succeeded, int connections) {
    MetricsSource source = ms.getSource("ShuffleMetrics");
    MetricsRecordBuilder rb = getMetrics(source);
    assertCounter("ShuffleOutputBytes", bytes, rb);
    assertCounter("ShuffleOutputsFailed", failed, rb);
    assertCounter("ShuffleOutputsOK", succeeded, rb);
    assertGauge("ShuffleConnections", connections, rb);
  }

  /**
   * Verify client prematurely closing a connection.
   *
   * @throws Exception exception.
   */
  @Test (timeout = 10000)
  public void testClientClosesConnection() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    ShuffleHandlerForTests shuffleHandler = new ShuffleHandlerForTests() {

      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String mapId, int reduce,
              String jobId, String user) {
            return null;
          }
          @Override
          protected void populateHeaders(List<String> mapIds, String jobId,
              String user, int reduce, HttpRequest request,
              HttpResponse response, boolean keepAliveParam,
              Map<String, MapOutputInfo> infoMap) {
            // Only set response headers and skip everything else
            // send some dummy value for content-length
            super.setResponseHeaders(response, keepAliveParam, 100);
          }
          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri) {
          }
          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
              Channel ch, String user, String mapId, int reduce,
              MapOutputInfo info)
                  throws IOException {
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i = 0; i < 100000; ++i) {
              header.write(dob);
            }
            return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.channel().close();
            }
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.channel().close();
            }
          }
        };
      }
    };
    shuffleHandler.init(conf);
    shuffleHandler.start();

    // simulate a reducer that closes early by reading a single shuffle header
    // then closing the connection
    URL url = new URL("http://127.0.0.1:"
        + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
        + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_1_0");
    HttpURLConnection conn = TEST_EXECUTION.openConnection(url);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    DataInputStream input = new DataInputStream(conn.getInputStream());
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    assertEquals("close",
        conn.getHeaderField(HttpHeader.CONNECTION.asString()));
    ShuffleHeader header = new ShuffleHeader();
    header.readFields(input);
    input.close();

    assertEquals("sendError called when client closed connection", 0,
        shuffleHandler.failures.size());
    assertEquals("Should have no caught exceptions", Collections.emptyList(),
        shuffleHandler.failures);

    shuffleHandler.stop();
  }

  static class LastSocketAddress {
    SocketAddress lastAddress;
    void setAddress(SocketAddress lastAddress) {
      this.lastAddress = lastAddress;
    }
    SocketAddress getSocketAddres() {
      return lastAddress;
    }
  }

  @Test(timeout = 10000)
  public void testKeepAliveInitiallyEnabled() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
        TEST_EXECUTION.getKeepAliveTimeout());
    ResponseConfig responseConfig = new ResponseConfig(HEADER_WRITE_COUNT, 0, 0);
    ShuffleHandlerForKeepAliveTests shuffleHandler = new ShuffleHandlerForKeepAliveTests(
        ATTEMPT_ID, responseConfig);
    testKeepAliveWithHttpOk(conf, shuffleHandler, ShuffleUrlType.SIMPLE,
        ShuffleUrlType.WITH_KEEPALIVE);
  }

  @Test(timeout = 1000000)
  public void testKeepAliveInitiallyEnabledTwoKeepAliveUrls() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
        TEST_EXECUTION.getKeepAliveTimeout());
    ResponseConfig responseConfig = new ResponseConfig(HEADER_WRITE_COUNT, 0, 0);
    ShuffleHandlerForKeepAliveTests shuffleHandler = new ShuffleHandlerForKeepAliveTests(
        ATTEMPT_ID, responseConfig);
    testKeepAliveWithHttpOk(conf, shuffleHandler, ShuffleUrlType.WITH_KEEPALIVE,
        ShuffleUrlType.WITH_KEEPALIVE);
  }

  //TODO snemeth implement keepalive test that used properly mocked ShuffleHandler
  @Test(timeout = 10000)
  public void testKeepAliveInitiallyDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, false);
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
        TEST_EXECUTION.getKeepAliveTimeout());
    ResponseConfig responseConfig = new ResponseConfig(HEADER_WRITE_COUNT, 0, 0);
    ShuffleHandlerForKeepAliveTests shuffleHandler = new ShuffleHandlerForKeepAliveTests(
        ATTEMPT_ID, responseConfig);
    testKeepAliveWithHttpOk(conf, shuffleHandler, ShuffleUrlType.WITH_KEEPALIVE,
        ShuffleUrlType.WITH_KEEPALIVE);
  }

  @Test(timeout = 10000)
  public void testKeepAliveMultipleMapAttemptIds() throws Exception {
    final int mapOutputContentLength = 11;
    final int mapOutputCount = 2;

    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
        TEST_EXECUTION.getKeepAliveTimeout());
    ResponseConfig responseConfig = new ResponseConfig(HEADER_WRITE_COUNT,
        mapOutputCount, mapOutputContentLength);
    ShuffleHandlerForKeepAliveTests shuffleHandler = new ShuffleHandlerForKeepAliveTests(
        ATTEMPT_ID, responseConfig);
    shuffleHandler.mapOutputSender.additionalMapOutputSenderOperations =
        new AdditionalMapOutputSenderOperations() {
          @Override
          public ChannelFuture perform(ChannelHandlerContext ctx, Channel ch) throws IOException {
            File tmpFile = File.createTempFile("test", ".tmp");
            Files.write(tmpFile.toPath(),
                "dummytestcontent123456".getBytes(StandardCharsets.UTF_8));
            final DefaultFileRegion partition = new DefaultFileRegion(tmpFile, 0,
                mapOutputContentLength);
            LOG.debug("Writing response partition: {}, channel: {}",
                partition, ch.id());
            return ch.writeAndFlush(partition)
                .addListener((ChannelFutureListener) future ->
                    LOG.debug("Finished Writing response partition: {}, channel: " +
                        "{}", partition, ch.id()));
          }
        };
    testKeepAliveWithHttpOk(conf, shuffleHandler,
        ShuffleUrlType.WITH_KEEPALIVE_MULTIPLE_MAP_IDS,
        ShuffleUrlType.WITH_KEEPALIVE_MULTIPLE_MAP_IDS);
  }

  @Test(timeout = 10000)
  public void testKeepAliveWithoutMapAttemptIds() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
        TEST_EXECUTION.getKeepAliveTimeout());
    ResponseConfig responseConfig = new ResponseConfig(HEADER_WRITE_COUNT, 0, 0);
    ShuffleHandlerForKeepAliveTests shuffleHandler = new ShuffleHandlerForKeepAliveTests(
        ATTEMPT_ID, responseConfig);
    shuffleHandler.setFailImmediatelyOnErrors(true);
    //Closing channels caused Netty to open another channel
    // so 1 request was handled with 2 separate channels,
    // ultimately generating 2 * HTTP 400 errors.
    // We'd like to avoid this so disabling closing the channel here.
    shuffleHandler.setCloseChannelOnError(false);
    testKeepAliveWithHttpBadRequest(conf, shuffleHandler, ShuffleUrlType.WITH_KEEPALIVE_NO_MAP_IDS);
  }

  private void testKeepAliveWithHttpOk(
      Configuration conf,
      ShuffleHandlerForKeepAliveTests shuffleHandler,
      ShuffleUrlType... shuffleUrlTypes) throws IOException {
    testKeepAliveWithHttpStatus(conf, shuffleHandler, shuffleUrlTypes, HttpURLConnection.HTTP_OK);
  }

  private void testKeepAliveWithHttpBadRequest(
      Configuration conf,
      ShuffleHandlerForKeepAliveTests shuffleHandler,
      ShuffleUrlType... shuffleUrlTypes) throws IOException {
    testKeepAliveWithHttpStatus(conf, shuffleHandler, shuffleUrlTypes,
        HttpURLConnection.HTTP_BAD_REQUEST);
  }

  private void testKeepAliveWithHttpStatus(Configuration conf,
      ShuffleHandlerForKeepAliveTests shuffleHandler,
      ShuffleUrlType[] shuffleUrlTypes,
      int expectedHttpStatus) throws IOException {
    if (expectedHttpStatus != HttpURLConnection.HTTP_BAD_REQUEST) {
      assertTrue("Expected at least two shuffle URL types ",
          shuffleUrlTypes.length >= 2);
    }
    shuffleHandler.init(conf);
    shuffleHandler.start();

    String[] urls = new String[shuffleUrlTypes.length];
    for (int i = 0; i < shuffleUrlTypes.length; i++) {
      ShuffleUrlType url = shuffleUrlTypes[i];
      if (url == ShuffleUrlType.SIMPLE) {
        urls[i] = getShuffleUrl(shuffleHandler, ATTEMPT_ID, ATTEMPT_ID);
      } else if (url == ShuffleUrlType.WITH_KEEPALIVE) {
        urls[i] = getShuffleUrlWithKeepAlive(shuffleHandler, ATTEMPT_ID, ATTEMPT_ID);
      } else if (url == ShuffleUrlType.WITH_KEEPALIVE_MULTIPLE_MAP_IDS) {
        urls[i] = getShuffleUrlWithKeepAlive(shuffleHandler, ATTEMPT_ID, ATTEMPT_ID, ATTEMPT_ID_2);
      } else if (url == ShuffleUrlType.WITH_KEEPALIVE_NO_MAP_IDS) {
        urls[i] = getShuffleUrlWithKeepAlive(shuffleHandler, ATTEMPT_ID);
      }
    }
    HttpConnectionHelper connHelper;
    try {
      connHelper = new HttpConnectionHelper(shuffleHandler.lastSocketAddress);
      connHelper.connectToUrls(urls, shuffleHandler.responseConfig, expectedHttpStatus);
      if (expectedHttpStatus == HttpURLConnection.HTTP_BAD_REQUEST) {
        assertEquals(1, shuffleHandler.failures.size());
        assertThat(shuffleHandler.failures.get(0).getMessage(),
            CoreMatchers.containsString("Status: 400 Bad Request, " +
                "message: Required param job, map and reduce"));
      }
    } finally {
      shuffleHandler.stop();
    }

    //Verify expectations
    int configuredTimeout = TEST_EXECUTION.getKeepAliveTimeout();
    int expectedTimeout = configuredTimeout < 0 ? 1 : configuredTimeout;

    connHelper.validate(connData -> {
      HttpConnectionAssert.create(connData)
          .expectKeepAliveWithTimeout(expectedTimeout)
          .expectResponseContentLength(shuffleHandler.responseConfig.contentLengthOfResponse);
    });
    if (expectedHttpStatus == HttpURLConnection.HTTP_OK) {
      HttpConnectionAssert.assertKeepAliveConnectionsAreSame(connHelper);
      assertEquals("Unexpected ShuffleHandler failure", Collections.emptyList(),
          shuffleHandler.failures);
    }
  }

  @Test(timeout = 10000)
  public void testSocketKeepAlive() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    // try setting to negative keep alive timeout.
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
        ARBITRARY_NEGATIVE_TIMEOUT_SECONDS);
    HttpURLConnection conn = null;
    MockShuffleHandler2 shuffleHandler = new MockShuffleHandler2();
    AuxiliaryLocalPathHandler pathHandler =
        mock(AuxiliaryLocalPathHandler.class);
    when(pathHandler.getLocalPathForRead(anyString())).thenThrow(
        new DiskChecker.DiskErrorException("Test"));
    shuffleHandler.setAuxiliaryLocalPathHandler(pathHandler);
    try {
      shuffleHandler.init(conf);
      shuffleHandler.start();

      String shuffleBaseURL = "http://127.0.0.1:"
              + shuffleHandler.getConfig().get(
                ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
      URL url =
          new URL(shuffleBaseURL + "/mapOutput?job=job_12345_1&reduce=1&"
              + "map=attempt_12345_1_m_1_0");
      conn = TEST_EXECUTION.openConnection(url);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      conn.connect();
      int rc = conn.getResponseCode();
      conn.getInputStream();
      assertEquals(HttpURLConnection.HTTP_OK, rc);
      assertTrue("socket should be set KEEP_ALIVE",
          shuffleHandler.isSocketKeepAlive());
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
      shuffleHandler.stop();
    }
    assertEquals("Should have no caught exceptions",
        Collections.emptyList(), shuffleHandler.failures);
  }

  /**
   * Simulate a reducer that sends an invalid shuffle-header - sometimes a wrong
   * header_name and sometimes a wrong version.
   * 
   * @throws Exception exception
   */
  @Test (timeout = 10000)
  public void testIncompatibleShuffleVersion() throws Exception {
    final int failureNum = 3;
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    ShuffleHandler shuffleHandler = new ShuffleHandlerForTests();
    shuffleHandler.init(conf);
    shuffleHandler.start();

    // simulate a reducer that closes early by reading a single shuffle header
    // then closing the connection
    URL url = new URL("http://127.0.0.1:"
        + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
        + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_1_0");
    for (int i = 0; i < failureNum; ++i) {
      HttpURLConnection conn = TEST_EXECUTION.openConnection(url);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          i == 0 ? "mapreduce" : "other");
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          i == 1 ? "1.0.0" : "1.0.1");
      conn.connect();
      assertEquals(
          HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
    }

    shuffleHandler.stop();
    shuffleHandler.close();
  }

  /**
   * Validate the limit on number of shuffle connections.
   * 
   * @throws Exception exception
   */
  @Test (timeout = 10000)
  public void testMaxConnections() throws Exception {
    final ArrayList<Throwable> failures = new ArrayList<>();
    final int maxAllowedConnections = 3;
    final int notAcceptedConnections = 1;
    final int connAttempts = maxAllowedConnections + notAcceptedConnections;
    
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, maxAllowedConnections);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String mapId, int reduce,
              String jobId, String user) {
            // Do nothing.
            return null;
          }
          @Override
          protected void populateHeaders(List<String> mapIds, String jobId,
              String user, int reduce, HttpRequest request,
              HttpResponse response, boolean keepAliveParam,
              Map<String, MapOutputInfo> infoMap) {
            // Do nothing.
          }
          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri) {
            // Do nothing.
          }
          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
              Channel ch, String user, String mapId, int reduce,
              MapOutputInfo info)
                  throws IOException {
            // send a shuffle header and a lot of data down the channel
            // to trigger a broken pipe
            ShuffleHeader header =
                new ShuffleHeader("dummy_header", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i=0; i<100000; ++i) {
              header.write(dob);
            }
            return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }

          @Override
          public void exceptionCaught(ChannelHandlerContext ctx,
              Throwable cause) throws Exception {
            LOG.debug("ExceptionCaught");
            failures.add(cause);
            super.exceptionCaught(ctx, cause);
          }
        };
      }
    };
    shuffleHandler.setUseOutboundExceptionHandler(true);
    shuffleHandler.init(conf);
    shuffleHandler.start();

    // setup connections
    HttpURLConnection[] conns = new HttpURLConnection[connAttempts];

    for (int i = 0; i < connAttempts; i++) {
      String urlString = "http://127.0.0.1:" 
           + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
           + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_"
           + i + "_0";
      URL url = new URL(urlString);
      conns[i] = TEST_EXECUTION.openConnection(url);
      conns[i].setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conns[i].setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    }

    // Try to open numerous connections
    for (int i = 0; i < connAttempts; i++) {
      conns[i].connect();
    }

    Map<Integer, List<HttpURLConnection>> mapOfConnections = Maps.newHashMap();
    for (HttpURLConnection conn : conns) {
      try {
        conn.getInputStream();
      } catch (IOException ioe) {
        LOG.info("Expected - connection should not be open");
      } catch (NumberFormatException ne) {
        fail("Expected a numerical value for RETRY_AFTER header field");
      } catch (Exception e) {
        fail("Expected a IOException");
      }
      int statusCode = conn.getResponseCode();
      LOG.debug("Connection status code: {}", statusCode);
      mapOfConnections.putIfAbsent(statusCode, new ArrayList<>());
      List<HttpURLConnection> connectionList = mapOfConnections.get(statusCode);
      connectionList.add(conn);
    }

    assertEquals(String.format("Expected only %s and %s response",
            OK_STATUS, ShuffleHandler.TOO_MANY_REQ_STATUS),
        Sets.newHashSet(
            HttpURLConnection.HTTP_OK,
            ShuffleHandler.TOO_MANY_REQ_STATUS.code()),
        mapOfConnections.keySet());
    
    List<HttpURLConnection> successfulConnections =
        mapOfConnections.get(HttpURLConnection.HTTP_OK);
    assertEquals(String.format("Expected exactly %d requests " +
            "with %s response", maxAllowedConnections, OK_STATUS),
        maxAllowedConnections, successfulConnections.size());

    //Ensure exactly one connection is HTTP 429 (TOO MANY REQUESTS)
    List<HttpURLConnection> closedConnections =
        mapOfConnections.get(ShuffleHandler.TOO_MANY_REQ_STATUS.code());
    assertEquals(String.format("Expected exactly %d %s response",
            notAcceptedConnections, ShuffleHandler.TOO_MANY_REQ_STATUS),
        notAcceptedConnections, closedConnections.size());

    // This connection should be closed because it is above the maximum limit
    HttpURLConnection conn = closedConnections.get(0);
    assertEquals(String.format("Expected a %s response",
            ShuffleHandler.TOO_MANY_REQ_STATUS),
        ShuffleHandler.TOO_MANY_REQ_STATUS.code(), conn.getResponseCode());
    long backoff = Long.parseLong(
        conn.getHeaderField(ShuffleHandler.RETRY_AFTER_HEADER));
    assertTrue("The backoff value cannot be negative.", backoff > 0);

    shuffleHandler.stop();

    //It's okay to get a ClosedChannelException.
    //All other kinds of exceptions means something went wrong
    assertEquals("Should have no caught exceptions",
        Collections.emptyList(), failures.stream()
            .filter(f -> !(f instanceof ClosedChannelException))
            .collect(toList()));
  }

  /**
   * Validate the ownership of the map-output files being pulled in. The
   * local-file-system owner of the file should match the user component in the
   *
   * @throws Exception exception
   */
  @Test(timeout = 100000)
  public void testMapFileAccess() throws IOException {
    final ArrayList<Throwable> failures = new ArrayList<>();
    // This will run only in NativeIO is enabled as SecureIOUtils need it
    assumeTrue(NativeIO.isAvailable());
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, ABS_LOG_DIR.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    LOG.info(appId.toString());
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    String reducerId = "0";
    List<File> fileMap = new ArrayList<>();
    createShuffleHandlerFiles(ABS_LOG_DIR, user, appId.toString(), appAttemptId,
        conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {

          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri) {
            // Do nothing.
          }

          @Override
          public void exceptionCaught(ChannelHandlerContext ctx,
              Throwable cause) throws Exception {
            LOG.debug("ExceptionCaught");
            failures.add(cause);
            super.exceptionCaught(ctx, cause);
          }

          @Override
          public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.pipeline().replace(HttpResponseEncoder.class,
                "loggingResponseEncoder",
                new LoggingHttpResponseEncoder(false));
            LOG.debug("Modified pipeline: {}", ctx.pipeline());
            super.channelActive(ctx);
          }
        };
      }
    };
    AuxiliaryLocalPathHandler pathHandler = new TestAuxiliaryLocalPathHandler();
    shuffleHandler.setUseOutboundExceptionHandler(true);
    shuffleHandler.setAuxiliaryLocalPathHandler(pathHandler);
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<>("identifier".getBytes(),
              "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler
          .initializeApplication(new ApplicationInitializationContext(user,
              appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
              outputBuffer.getLength())));
      URL url =
          new URL(
              "http://127.0.0.1:"
                  + shuffleHandler.getConfig().get(
                      ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                  + "/mapOutput?job=job_12345_0001&reduce=" + reducerId
                  + "&map=attempt_12345_1_m_1_0");
      HttpURLConnection conn = TEST_EXECUTION.openConnection(url);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      conn.connect();
      DataInputStream is = new DataInputStream(conn.getInputStream());
      InputStreamReadResult result = HttpConnectionHelper.readDataFromInputStream(is);
      String receivedString = result.asString;

      //Retrieve file owner name
      FileInputStream fis = new FileInputStream(fileMap.get(0));
      String owner = NativeIO.POSIX.getFstat(fis.getFD()).getOwner();
      fis.close();

      String message =
          "Owner '" + owner + "' for path " + fileMap.get(0).getAbsolutePath()
              + " did not match expected owner '" + user + "'";
      assertTrue(String.format("Received string '%s' should contain " +
          "message '%s'", receivedString, message),
          receivedString.contains(message));
      assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
      LOG.info("received: " + receivedString);
      assertNotEquals("", receivedString);
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(ABS_LOG_DIR);
    }

    assertEquals("Should have no caught exceptions",
        Collections.emptyList(), failures);
  }

  private static void createShuffleHandlerFiles(File logDir, String user,
      String appId, String appAttemptId, Configuration conf,
      List<File> fileMap) throws IOException {
    String attemptDir =
        StringUtils.join(Path.SEPARATOR,
            new String[] {logDir.getAbsolutePath(),
                ContainerLocalizer.USERCACHE, user,
                ContainerLocalizer.APPCACHE, appId, "output", appAttemptId });
    File appAttemptDir = new File(attemptDir);
    appAttemptDir.mkdirs();
    System.out.println(appAttemptDir.getAbsolutePath());
    File indexFile = new File(appAttemptDir, "file.out.index");
    fileMap.add(indexFile);
    createIndexFile(indexFile, conf);
    File mapOutputFile = new File(appAttemptDir, "file.out");
    fileMap.add(mapOutputFile);
    createMapOutputFile(mapOutputFile, conf);
  }

  private static void createMapOutputFile(File mapOutputFile, Configuration conf)
          throws IOException {
    FileOutputStream out = new FileOutputStream(mapOutputFile);
    out.write("Creating new dummy map output file. Used only for testing"
        .getBytes());
    out.flush();
    out.close();
  }

  private static void createIndexFile(File indexFile, Configuration conf)
      throws IOException {
    if (indexFile.exists()) {
      System.out.println("Deleting existing file");
      indexFile.delete();
    }
    indexFile.createNewFile();
    FSDataOutputStream output = FileSystem.getLocal(conf).getRaw().append(
        new Path(indexFile.getAbsolutePath()));
    Checksum crc = new PureJavaCrc32();
    crc.reset();
    CheckedOutputStream chk = new CheckedOutputStream(output, crc);
    String msg = "Writing new index file. This file will be used only " +
        "for the testing.";
    chk.write(Arrays.copyOf(msg.getBytes(),
        MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH));
    output.writeLong(chk.getChecksum().getValue());
    output.close();
  }

  @Test
  public void testRecovery() throws IOException {
    final String user = "someuser";
    final ApplicationId appId = ApplicationId.newInstance(12345, 1);
    final JobID jobId = JobID.downgrade(TypeConverter.fromYarn(appId));
    final File tmpDir = new File(System.getProperty("test.build.data",
        System.getProperty("java.io.tmpdir")),
        TestShuffleHandler.class.getName());
    ShuffleHandler shuffle = new ShuffleHandlerForTests();
    AuxiliaryLocalPathHandler pathHandler = new TestAuxiliaryLocalPathHandler();
    shuffle.setAuxiliaryLocalPathHandler(pathHandler);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS,
        ABS_LOG_DIR.getAbsolutePath());
    // emulate aux services startup with recovery enabled
    shuffle.setRecoveryPath(new Path(tmpDir.toString()));
    tmpDir.mkdirs();
    try {
      shuffle.init(conf);
      shuffle.start();

      // set up a shuffle token for an application
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt = new Token<>(
          "identifier".getBytes(), "password".getBytes(), new Text(user),
          new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffle.initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
            outputBuffer.getLength())));

      // verify we are authorized to shuffle
      int rc = getShuffleResponseCode(shuffle, jt);
      assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setAuxiliaryLocalPathHandler(pathHandler);
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(shuffle, jt);
      assertEquals(HttpURLConnection.HTTP_OK, rc);

      // shutdown app and verify access is lost
      shuffle.stopApplication(new ApplicationTerminationContext(appId));
      rc = getShuffleResponseCode(shuffle, jt);
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we still don't have access
      rc = getShuffleResponseCode(shuffle, jt);
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, rc);
    } finally {
      if (shuffle != null) {
        shuffle.close();
      }
      FileUtil.fullyDelete(tmpDir);
    }
  }
  
  @Test
  public void testRecoveryFromOtherVersions() throws IOException {
    final String user = "someuser";
    final ApplicationId appId = ApplicationId.newInstance(12345, 1);
    final File tmpDir = new File(System.getProperty("test.build.data",
        System.getProperty("java.io.tmpdir")),
        TestShuffleHandler.class.getName());
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    ShuffleHandler shuffle = new ShuffleHandlerForTests();
    AuxiliaryLocalPathHandler pathHandler = new TestAuxiliaryLocalPathHandler();
    shuffle.setAuxiliaryLocalPathHandler(pathHandler);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, ABS_LOG_DIR.getAbsolutePath());
    // emulate aux services startup with recovery enabled
    shuffle.setRecoveryPath(new Path(tmpDir.toString()));
    tmpDir.mkdirs();
    try {
      shuffle.init(conf);
      shuffle.start();

      // set up a shuffle token for an application
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt = new Token<>(
          "identifier".getBytes(), "password".getBytes(), new Text(user),
          new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffle.initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
              outputBuffer.getLength())));

      // verify we are authorized to shuffle
      int rc = getShuffleResponseCode(shuffle, jt);
      assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setAuxiliaryLocalPathHandler(pathHandler);
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(shuffle, jt);
      assertEquals(HttpURLConnection.HTTP_OK, rc);
      Version version = Version.newInstance(1, 0);
      assertEquals(version, shuffle.getCurrentVersion());
    
      // emulate shuffle handler restart with compatible version
      Version version11 = Version.newInstance(1, 1);
      // update version info before close shuffle
      shuffle.storeVersion(version11);
      assertEquals(version11, shuffle.loadVersion());
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setAuxiliaryLocalPathHandler(pathHandler);
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();
      // shuffle version will be override by CURRENT_VERSION_INFO after restart
      // successfully.
      assertEquals(version, shuffle.loadVersion());
      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(shuffle, jt);
      assertEquals(HttpURLConnection.HTTP_OK, rc);
    
      // emulate shuffle handler restart with incompatible version
      Version version21 = Version.newInstance(2, 1);
      shuffle.storeVersion(version21);
      assertEquals(version21, shuffle.loadVersion());
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setAuxiliaryLocalPathHandler(pathHandler);
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
    
      try {
        shuffle.start();
        fail("Incompatible version, should expect fail here.");
      } catch (ServiceStateException e) {
        assertTrue("Exception message mismatch",
            e.getMessage().contains("Incompatible version for state DB schema:"));
      } 
    
    } finally {
      if (shuffle != null) {
        shuffle.close();
      }
      FileUtil.fullyDelete(tmpDir);
    }
  }

  private static int getShuffleResponseCode(ShuffleHandler shuffle,
      Token<JobTokenIdentifier> jt) throws IOException {
    URL url = new URL("http://127.0.0.1:"
        + shuffle.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
        + "/mapOutput?job=job_12345_0001&reduce=0&map=attempt_12345_1_m_1_0");
    HttpURLConnection conn = TEST_EXECUTION.openConnection(url);
    String encHash = SecureShuffleUtils.hashFromString(
        SecureShuffleUtils.buildMsgFrom(url),
        JobTokenSecretManager.createSecretKey(jt.getPassword()));
    conn.addRequestProperty(
        SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    int rc = conn.getResponseCode();
    conn.disconnect();
    return rc;
  }

  @Test(timeout = 100000)
  public void testGetMapOutputInfo() throws Exception {
    final ArrayList<Throwable> failures = new ArrayList<>(1);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "simple");
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, ABS_LOG_DIR.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    String reducerId = "0";
    List<File> fileMap = new ArrayList<>();
    createShuffleHandlerFiles(ABS_LOG_DIR, user, appId.toString(), appAttemptId,
        conf, fileMap);
    AuxiliaryLocalPathHandler pathHandler = new TestAuxiliaryLocalPathHandler();
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected void populateHeaders(List<String> mapIds,
              String outputBaseStr, String user, int reduce,
              HttpRequest request, HttpResponse response,
              boolean keepAliveParam, Map<String, MapOutputInfo> infoMap)
              throws IOException {
            // Only set response headers and skip everything else
            // send some dummy value for content-length
            super.setResponseHeaders(response, keepAliveParam, 100);
          }
          @Override
          protected void verifyRequest(String appid,
              ChannelHandlerContext ctx, HttpRequest request,
              HttpResponse response, URL requestUri) {
            // Do nothing.
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error(message));
              ctx.channel().close();
            }
          }
          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
              Channel ch, String user, String mapId, int reduce,
              MapOutputInfo info) throws IOException {
            // send a shuffle header
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }
        };
      }
    };
    shuffleHandler.setUseOutboundExceptionHandler(true);
    shuffleHandler.setAuxiliaryLocalPathHandler(pathHandler);
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<>("identifier".getBytes(),
              "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler
          .initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
          outputBuffer.getLength())));
      URL url =
          new URL(
              "http://127.0.0.1:"
                  + shuffleHandler.getConfig().get(
                      ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                  + "/mapOutput?job=job_12345_0001&reduce=" + reducerId
                  + "&map=attempt_12345_1_m_1_0");
      HttpURLConnection conn = TEST_EXECUTION.openConnection(url);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      conn.connect();
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        ShuffleHeader header = new ShuffleHeader();
        header.readFields(is);
        is.close();
      } catch (EOFException e) {
        // ignore
      }
      assertEquals("sendError called due to shuffle error",
          0, failures.size());
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(ABS_LOG_DIR);
    }
  }

  @Test(timeout = 4000)
  public void testSendMapCount() throws Exception {
    final List<ShuffleHandler.ReduceMapFileCount> listenerList =
        new ArrayList<>();
    int connectionKeepAliveTimeOut = 5; //arbitrary value
    final ChannelHandlerContext mockCtx =
        mock(ChannelHandlerContext.class);
    final Channel mockCh = mock(AbstractChannel.class);
    final ChannelPipeline mockPipeline = mock(ChannelPipeline.class);

    // Mock HttpRequest and ChannelFuture
    final HttpRequest mockHttpRequest = createMockHttpRequest();
    final ChannelFuture mockFuture = createMockChannelFuture(mockCh,
        listenerList);
    final ShuffleHandler.TimeoutHandler timerHandler =
        new ShuffleHandler.TimeoutHandler(connectionKeepAliveTimeOut);

    // Mock Netty Channel Context and Channel behavior
    Mockito.doReturn(mockCh).when(mockCtx).channel();
    when(mockCh.pipeline()).thenReturn(mockPipeline);
    when(mockPipeline.get(
        Mockito.any(String.class))).thenReturn(timerHandler);
    when(mockCtx.channel()).thenReturn(mockCh);
    Mockito.doReturn(mockFuture).when(mockCh).writeAndFlush(Mockito.any(Object.class));

    final MockShuffleHandler sh = new MockShuffleHandler();
    Configuration conf = new Configuration();
    sh.init(conf);
    sh.start();
    int maxOpenFiles =conf.getInt(ShuffleHandler.SHUFFLE_MAX_SESSION_OPEN_FILES,
        ShuffleHandler.DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES);
    sh.getShuffle(conf).channelRead(mockCtx, mockHttpRequest);
    assertTrue("Number of Open files should not exceed the configured " +
            "value!-Not Expected",
        listenerList.size() <= maxOpenFiles);
    while(!listenerList.isEmpty()) {
      listenerList.remove(0).operationComplete(mockFuture);
      assertTrue("Number of Open files should not exceed the configured " +
              "value!-Not Expected",
          listenerList.size() <= maxOpenFiles);
    }
    sh.close();
    sh.stop();

    assertEquals("Should have no caught exceptions",
        Collections.emptyList(), sh.failures);
  }

  @Test(timeout = 10000)
  public void testIdleStateHandlingSpecifiedTimeout() throws Exception {
    int timeoutSeconds = 4;
    int expectedTimeoutSeconds = timeoutSeconds;
    testHandlingIdleState(timeoutSeconds, expectedTimeoutSeconds);
  }

  @Test(timeout = 10000)
  public void testIdleStateHandlingNegativeTimeoutDefaultsTo1Second() throws Exception {
    int expectedTimeoutSeconds = 1; //expected by production code
    testHandlingIdleState(ARBITRARY_NEGATIVE_TIMEOUT_SECONDS, expectedTimeoutSeconds);
  }

  private String getShuffleUrlWithKeepAlive(ShuffleHandler shuffleHandler, long jobId,
      long... attemptIds) {
    String url = getShuffleUrl(shuffleHandler, jobId, attemptIds);
    return url + "&keepAlive=true";
  }

  private String getShuffleUrl(ShuffleHandler shuffleHandler, long jobId, long... attemptIds) {
    String port = shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
    String shuffleBaseURL = "http://127.0.0.1:" + port;

    StringBuilder mapAttemptIds = new StringBuilder();
    for (int i = 0; i < attemptIds.length; i++) {
      if (i == 0) {
        mapAttemptIds.append("&map=");
      } else {
        mapAttemptIds.append(",");
      }
      mapAttemptIds.append(String.format("attempt_%s_1_m_1_0", attemptIds[i]));
    }

    String location = String.format("/mapOutput" +
        "?job=job_%s_1" +
        "&reduce=1" +
        "%s", jobId, mapAttemptIds);
    return shuffleBaseURL + location;
  }

  private void testHandlingIdleState(int configuredTimeoutSeconds, int expectedTimeoutSeconds)
      throws IOException,
      InterruptedException {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, TEST_EXECUTION.shuffleHandlerPort());
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, configuredTimeoutSeconds);

    final CountDownLatch countdownLatch = new CountDownLatch(1);
    ResponseConfig responseConfig = new ResponseConfig(HEADER_WRITE_COUNT, 0, 0);
    ShuffleHandlerForKeepAliveTests shuffleHandler = new ShuffleHandlerForKeepAliveTests(
        ATTEMPT_ID, responseConfig,
        event -> countdownLatch.countDown());
    shuffleHandler.init(conf);
    shuffleHandler.start();

    String shuffleUrl = getShuffleUrl(shuffleHandler, ATTEMPT_ID, ATTEMPT_ID);
    String[] urls = new String[] {shuffleUrl};
    HttpConnectionHelper httpConnectionHelper = new HttpConnectionHelper(
        shuffleHandler.lastSocketAddress);
    long beforeConnectionTimestamp = System.currentTimeMillis();
    httpConnectionHelper.connectToUrls(urls, shuffleHandler.responseConfig);
    countdownLatch.await();
    long channelClosedTimestamp = System.currentTimeMillis();
    long secondsPassed =
        TimeUnit.SECONDS.convert(channelClosedTimestamp - beforeConnectionTimestamp,
            TimeUnit.MILLISECONDS);
    assertTrue(String.format("Expected at least %s seconds of timeout. " +
            "Actual timeout seconds: %s", expectedTimeoutSeconds, secondsPassed),
        secondsPassed >= expectedTimeoutSeconds);
    shuffleHandler.stop();
  }

  public ChannelFuture createMockChannelFuture(Channel mockCh,
      final List<ShuffleHandler.ReduceMapFileCount> listenerList) {
    final ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockFuture.channel()).thenReturn(mockCh);
    Mockito.doReturn(true).when(mockFuture).isSuccess();
    Mockito.doAnswer(invocation -> {
      //Add ReduceMapFileCount listener to a list
      if (invocation.getArguments()[0].getClass() == ShuffleHandler.ReduceMapFileCount.class) {
        listenerList.add((ShuffleHandler.ReduceMapFileCount)
            invocation.getArguments()[0]);
      }
      return null;
    }).when(mockFuture).addListener(Mockito.any(
        ShuffleHandler.ReduceMapFileCount.class));
    return mockFuture;
  }

  public HttpRequest createMockHttpRequest() {
    HttpRequest mockHttpRequest = mock(HttpRequest.class);
    Mockito.doReturn(HttpMethod.GET).when(mockHttpRequest).method();
    Mockito.doAnswer(invocation -> {
      String uri = "/mapOutput?job=job_12345_1&reduce=1";
      for (int i = 0; i < 100; i++) {
        uri = uri.concat("&map=attempt_12345_1_m_" + i + "_0");
      }
      return uri;
    }).when(mockHttpRequest).uri();
    return mockHttpRequest;
  }
}
