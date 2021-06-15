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

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpHeaders;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
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
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
  private static final int DEFAULT_PORT = 0;

  //TODO snemeth Disable debug mode when creating patch
  //Control test execution properties with these flags
  private static final boolean DEBUG_MODE = true;
  //If this is set to true and proxy server is not running, tests will fail!
  private static final boolean USE_PROXY = false; 
  private static final int HEADER_WRITE_COUNT = 100000;
  private static TestExecution TEST_EXECUTION;

  private static class TestExecution {
    private static final int DEFAULT_KEEP_ALIVE_TIMEOUT = -100;
    private static final int DEBUG_FRIENDLY_KEEP_ALIVE = 1000;
    private static final String PROXY_HOST = "127.0.0.1";
    private static final int PROXY_PORT = 8888;
    private boolean debugMode;
    private boolean useProxy;

    public TestExecution(boolean debugMode, boolean useProxy) {
      this.debugMode = debugMode;
      this.useProxy = useProxy;
    }

    int getKeepAliveTimeout() {
      if (debugMode) {
        return DEBUG_FRIENDLY_KEEP_ALIVE;
      }
      return DEFAULT_KEEP_ALIVE_TIMEOUT;
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
  }
  
  private enum ShuffleUrlType {
    SIMPLE, WITH_KEEPALIVE
  }

  private static class InputStreamReadResult {
    final String asString;
    final int totalBytesRead;

    public InputStreamReadResult(byte[] bytes, int totalBytesRead) {
      this.asString = new String(bytes, StandardCharsets.UTF_8);
      this.totalBytesRead = totalBytesRead;
    }
  }

  private class ShuffleHandlerForKeepAliveTests extends ShuffleHandler {
    final int headerWriteCount;
    final LastSocketAddress lastSocketAddress = new LastSocketAddress();
    final ArrayList<Throwable> failures = new ArrayList<>();
    final ShuffleHeaderProvider shuffleHeaderProvider;
    final HeaderPopulator headerPopulator;
    final MapOutputSender mapOutputSender;
    private final int expectedResponseSize;
    private Consumer<IdleStateEvent> channelIdleCallback;
    private CustomTimeoutHandler customTimeoutHandler;

    public ShuffleHandlerForKeepAliveTests(int headerWriteCount, long attemptId,
        Consumer<IdleStateEvent> channelIdleCallback) throws IOException {
      this(headerWriteCount, attemptId);
      this.channelIdleCallback = channelIdleCallback;
    }

    public ShuffleHandlerForKeepAliveTests(int headerWriteCount, long attemptId) throws IOException {
      this.headerWriteCount = headerWriteCount;
      shuffleHeaderProvider = new ShuffleHeaderProvider(attemptId);
      headerPopulator = new HeaderPopulator(this, headerWriteCount, true,
          shuffleHeaderProvider);
      mapOutputSender = new MapOutputSender(this, headerWriteCount, lastSocketAddress, shuffleHeaderProvider);
      int headerSize = getShuffleHeaderSize(shuffleHeaderProvider);
      this.expectedResponseSize = headerWriteCount * headerSize;
      setUseOutboundExceptionHandler(true);
    }

    private int getShuffleHeaderSize(ShuffleHeaderProvider shuffleHeaderProvider) throws IOException {
      DataOutputBuffer dob = new DataOutputBuffer();
      ShuffleHeader header =
          shuffleHeaderProvider.createNewShuffleHeader();
      header.write(dob);
      return dob.size();
    }

    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      // replace the shuffle handler with one stubbed for testing
      return new Shuffle(conf) {
        @Override
        protected MapOutputInfo getMapOutputInfo(String mapId, int reduce,
            String jobId, String user) throws IOException {
          return null;
        }
        @Override
        protected void verifyRequest(String appid, ChannelHandlerContext ctx,
            HttpRequest request, HttpResponse response, URL requestUri)
            throws IOException {
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
          ctx.pipeline().replace(HttpResponseEncoder.class, "loggingResponseEncoder", new LoggingHttpResponseEncoder(false));
          replaceTimeoutHandlerWithCustom(ctx);
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
          if (failures.size() == 0) {
            failures.add(new Error());
            LOG.warn("sendError: Closing channel");
            ctx.channel().close();
          }
        }

        @Override
        protected void sendError(ChannelHandlerContext ctx, String message,
            HttpResponseStatus status) {
          if (failures.size() == 0) {
            failures.add(new Error());
            LOG.warn("sendError: Closing channel");
            ctx.channel().close();
          }
        }
      };
    }

    private class CustomTimeoutHandler extends TimeoutHandler {
      private boolean channelIdle = false;
      private final Consumer<IdleStateEvent> channelIdleCallback;

      public CustomTimeoutHandler(int connectionKeepAliveTimeOut,
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

  static class LoggingHttpResponseEncoder extends HttpResponseEncoder {
    private final boolean logStacktraceOfEncodingMethods;

    public LoggingHttpResponseEncoder(boolean logStacktraceOfEncodingMethods) {
      this.logStacktraceOfEncodingMethods = logStacktraceOfEncodingMethods;
    }

    @Override
    public boolean acceptOutboundMessage(Object msg) throws Exception {
      printExecutingMethod();
      return super.acceptOutboundMessage(msg);
    }

    @Override
    protected void encodeInitialLine(ByteBuf buf, HttpResponse response) throws Exception {
      LOG.debug("Executing method: {}, response: {}",
          getExecutingMethodName(), response);
      logStacktraceIfRequired();
      super.encodeInitialLine(buf, response);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg,
        List<Object> out) throws Exception {
      printExecutingMethod();
      logStacktraceIfRequired();
      super.encode(ctx, msg, out);
    }

    @Override
    protected void encodeHeaders(HttpHeaders headers, ByteBuf buf) {
      printExecutingMethod();
      super.encodeHeaders(headers, buf);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise
        promise) throws Exception {
      printExecutingMethod();
      super.write(ctx, msg, promise);
    }

    private void logStacktraceIfRequired() {
      if (logStacktraceOfEncodingMethods) {
        LOG.debug("Stacktrace: ", new Throwable());
      }
    }

    private void printExecutingMethod() {
      String methodName = getExecutingMethodName();
      LOG.debug("Executing method: {}", methodName);
    }

    private String getExecutingMethodName() {
      StackTraceElement[] stackTrace = Thread.currentThread()
          .getStackTrace();
      // Array items (indices):
      // 0: java.lang.Thread.getStackTrace(...)
      // 1: TestShuffleHandler$LoggingHttpResponseEncoder.getExecutingMethodName(...)
      String methodName = stackTrace[2].getMethodName();
      //If this method was called from printExecutingMethod, 
      // we have yet another stack frame
      if (methodName.endsWith("printExecutingMethod")) {
        methodName = stackTrace[3].getMethodName();
      }
      String className = this.getClass().getSimpleName();
      return className + "#" + methodName;
    }
  }

  private static class MapOutputSender {
    private final ShuffleHandler shuffleHandler;
    private int headerWriteCount;
    private final LastSocketAddress lastSocketAddress;
    private ShuffleHeaderProvider shuffleHeaderProvider;

    public MapOutputSender(ShuffleHandler shuffleHandler,
        int headerWriteCount, LastSocketAddress lastSocketAddress,
        ShuffleHeaderProvider shuffleHeaderProvider) {
      this.shuffleHandler = shuffleHandler;
      this.headerWriteCount = headerWriteCount;
      this.lastSocketAddress = lastSocketAddress;
      this.shuffleHeaderProvider = shuffleHeaderProvider;
    }

    public ChannelFuture send(ChannelHandlerContext ctx, Channel ch) throws IOException {
      LOG.debug("In MapOutputSender#send");
      lastSocketAddress.setAddress(ch.remoteAddress());
      ShuffleHeader header =
          shuffleHeaderProvider.createNewShuffleHeader();
      writeOneHeader(ch, header);
      ChannelFuture future = writeHeaderNTimes(ch, header,
          headerWriteCount);
      // This is the last operation
      // It's safe to increment ShuffleHeader counter for better identification
      shuffleHeaderProvider.incrementCounter();
      return future;
    }

    private void writeOneHeader(Channel ch, ShuffleHeader header) throws IOException {
      DataOutputBuffer dob = new DataOutputBuffer();
      header.write(dob);
      LOG.debug("MapOutputSender#writeOneHeader before WriteAndFlush #1");
      ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
      LOG.debug("MapOutputSender#writeOneHeader after WriteAndFlush #1. outputBufferSize: " + dob.size());
    }

    private ChannelFuture writeHeaderNTimes(Channel ch, ShuffleHeader header, int iterations) throws IOException {
      DataOutputBuffer dob = new DataOutputBuffer();
      for (int i = 0; i < iterations; ++i) {
        header.write(dob);
      }
      LOG.debug("MapOutputSender#writeHeaderNTimes WriteAndFlush big chunk of data, outputBufferSize: " + dob.size());
      return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0,
          dob.getLength()));
    }
  }

  private static class ShuffleHeaderProvider {
    private final long attemptId;
    private final AtomicInteger attemptCounter;

    public ShuffleHeaderProvider(long attemptId) {
      this.attemptId = attemptId;
      this.attemptCounter = new AtomicInteger();
    }

    ShuffleHeader createNewShuffleHeader() {
      return new ShuffleHeader(String.format("attempt_%s_1_m_1_0%s", attemptId,
          attemptCounter.get()), 5678, 5678, 1);
    }

    void incrementCounter() {
      attemptCounter.incrementAndGet();
    }
  }

  private static class HeaderPopulator {
    private ShuffleHandler shuffleHandler;
    private final int headerWriteCount;
    private boolean disableKeepAliveConfig;
    private ShuffleHeaderProvider shuffleHeaderProvider;

    public HeaderPopulator(ShuffleHandler shuffleHandler,
        int headerWriteCount,
        boolean disableKeepAliveConfig,
        ShuffleHeaderProvider shuffleHeaderProvider) {
      this.shuffleHandler = shuffleHandler;
      this.headerWriteCount = headerWriteCount;
      this.disableKeepAliveConfig = disableKeepAliveConfig;
      this.shuffleHeaderProvider = shuffleHeaderProvider;
    }

    public long populateHeaders(boolean keepAliveParam) throws IOException {
      // Send some dummy data (populate content length details)
      DataOutputBuffer dob = new DataOutputBuffer();
      for (int i = 0; i < headerWriteCount; ++i) {
        ShuffleHeader header =
            shuffleHeaderProvider.createNewShuffleHeader();
        header.write(dob);
      }
      long contentLength = dob.getLength();
      LOG.debug("HTTP response content length: {}", contentLength);
      // for testing purpose;
      // disable connectionKeepAliveEnabled if keepAliveParam is available
      if (keepAliveParam && disableKeepAliveConfig) {
        shuffleHandler.connectionKeepAliveEnabled = false;
      }
      return contentLength;
    }
  }

  private static class HttpConnectionData {
    private final Map<String, List<String>> headers;
    private HttpURLConnection conn;
    private int payloadLength;
    private SocketAddress socket;
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
        Assert.fail("Failed to read response code from connection: " + conn);
      }
    }

    static HttpConnectionData create(HttpURLConnection conn, int payloadLength, SocketAddress socket) {
      return new HttpConnectionData(conn, payloadLength, socket);
    }
  }

  private static class HttpConnectionAssert {
    private final HttpConnectionData connData;

    private HttpConnectionAssert(HttpConnectionData connData) {
      this.connData = connData;
    }

    static HttpConnectionAssert create(HttpConnectionData connData) {
      return new HttpConnectionAssert(connData);
    }

    public static void assertKeepAliveConnectionsAreSame(HttpConnectionHelper httpConnectionHelper) {
      Assert.assertTrue("At least two connection data " +
          "is required to perform this assertion",
          httpConnectionHelper.connectionData.size() >= 2);
      SocketAddress firstAddress = httpConnectionHelper.getConnectionData(0).socket;
      SocketAddress secondAddress = httpConnectionHelper.getConnectionData(1).socket;
      Assert.assertNotNull("Initial shuffle address should not be null",
          firstAddress);
      Assert.assertNotNull("Keep-Alive shuffle address should not be null",
          secondAddress);
      Assert.assertEquals("Initial shuffle address and keep-alive shuffle "
          + "address should be the same", firstAddress, secondAddress);
    }

    public HttpConnectionAssert expectKeepAliveWithTimeout(long timeout) {
      Assert.assertEquals(HttpURLConnection.HTTP_OK, connData.responseCode);
      assertHeaderValue(HttpHeader.CONNECTION, HttpHeader.KEEP_ALIVE.asString());
      assertHeaderValue(HttpHeader.KEEP_ALIVE, "timeout=" + timeout);
      return this;
    }

    public HttpConnectionAssert expectResponseSize(int size) {
      Assert.assertEquals(size, connData.payloadLength);
      return this;
    }

    private void assertHeaderValue(HttpHeader header, String expectedValue) {
      List<String> headerList = connData.headers.get(header.asString());
      Assert.assertNotNull("Got null header value for header: " + header, headerList);
      Assert.assertFalse("Got empty header value for header: " + header, headerList.isEmpty());
      assertEquals("Unexpected size of header list for header: " + header, 1,
          headerList.size());
      Assert.assertEquals(expectedValue, headerList.get(0));
    }
  }

  private static class HttpConnectionHelper {
    private final LastSocketAddress lastSocketAddress;
    List<HttpConnectionData> connectionData = new ArrayList<>();

    public HttpConnectionHelper(LastSocketAddress lastSocketAddress) {
      this.lastSocketAddress = lastSocketAddress;
    }

    public void connectToUrls(String[] urls) throws IOException {
      int requests = urls.length;
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
        conn.connect();
        DataInputStream input = new DataInputStream(conn.getInputStream());
        LOG.debug("Opened DataInputStream for connection: {}/{}", (reqIdx + 1), requests);
        ShuffleHeader header = new ShuffleHeader();
        header.readFields(input);
        InputStreamReadResult result = readDataFromInputStream(input);
        connectionData.add(HttpConnectionData
            .create(conn, result.totalBytesRead, lastSocketAddress.getSocketAddres()));
        input.close();
      }

      Assert.assertEquals(urls.length, connectionData.size());
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
        DataInputStream input) throws IOException {
      ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int bytesRead;
      int totalBytesRead = 0;
      while ((bytesRead = input.read(buffer)) != -1) {
        dataStream.write(buffer);
        totalBytesRead += bytesRead;
      }
      LOG.debug("Read total bytes: " + totalBytesRead);
      dataStream.flush();
      return new InputStreamReadResult(dataStream.toByteArray(), totalBytesRead);
    }
  }

  class ShuffleHandlerForTests extends ShuffleHandler {
    final ArrayList<Throwable> failures = new ArrayList<>();

    public ShuffleHandlerForTests() {
      setUseOutboundExceptionHandler(true);
    }

    public ShuffleHandlerForTests(MetricsSystem ms) {
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

    private AuxiliaryLocalPathHandler pathHandler =
        new TestAuxiliaryLocalPathHandler();

    public MockShuffleHandler() {
      setUseOutboundExceptionHandler(true);
    }

    public MockShuffleHandler(MetricsSystem ms) {
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
            String jobId, String user) throws IOException {
          // Do nothing.
          return null;
        }
        @Override
        protected void populateHeaders(List<String> mapIds, String jobId,
            String user, int reduce, HttpRequest request,
            HttpResponse response, boolean keepAliveParam,
            Map<String, MapOutputInfo> infoMap) throws IOException {
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
    public Path getLocalPathForRead(String path) throws IOException {
      return new Path(ABS_LOG_DIR.getAbsolutePath(), path);
    }

    @Override
    public Path getLocalPathForWrite(String path) throws IOException {
      return new Path(ABS_LOG_DIR.getAbsolutePath());
    }

    @Override
    public Path getLocalPathForWrite(String path, long size)
        throws IOException {
      return new Path(ABS_LOG_DIR.getAbsolutePath());
    }

    @Override
    public Iterable<Path> getAllLocalPathsForRead(String path)
        throws IOException {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(ABS_LOG_DIR.getAbsolutePath()));
      return paths;
    }
  }

  private static class MockShuffleHandler2 extends
      org.apache.hadoop.mapred.ShuffleHandler {
    final ArrayList<Throwable> failures = new ArrayList<>(1);

    public MockShuffleHandler2() {
      setUseOutboundExceptionHandler(true);
    }

    public MockShuffleHandler2(MetricsSystem ms) {
      super(ms);
      setUseOutboundExceptionHandler(true);
    }

    boolean socketKeepAlive = false;
    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      return new Shuffle(conf) {
        @Override
        protected void verifyRequest(String appid, ChannelHandlerContext ctx,
            HttpRequest request, HttpResponse response, URL requestUri)
            throws IOException {
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

  @Before
  public void setup() {
    TEST_EXECUTION = new TestExecution(DEBUG_MODE, USE_PROXY);
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
    sh.metrics.shuffleOutputBytes.incr(1*MiB);
    sh.metrics.shuffleConnections.incr();
    sh.metrics.shuffleOutputBytes.incr(2*MiB);

    checkShuffleMetrics(ms, 3*MiB, 0 , 0, 2);

    sh.metrics.operationComplete(cf);
    sh.metrics.operationComplete(cf);

    checkShuffleMetrics(ms, 3*MiB, 1, 1, 0);
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
    final ArrayList<Throwable> failures = new ArrayList<Throwable>(1);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
    ShuffleHandler shuffleHandler = new ShuffleHandlerForTests() {

      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String mapId, int reduce,
              String jobId, String user) throws IOException {
            return null;
          }
          @Override
          protected void populateHeaders(List<String> mapIds, String jobId,
              String user, int reduce, HttpRequest request,
              HttpResponse response, boolean keepAliveParam,
              Map<String, MapOutputInfo> infoMap) throws IOException {
            // Only set response headers and skip everything else
            // send some dummy value for content-length
            super.setResponseHeaders(response, keepAliveParam, 100);
          }
          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri)
                  throws IOException {
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
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    Assert.assertEquals("close",
        conn.getHeaderField(HttpHeader.CONNECTION.asString()));
    ShuffleHeader header = new ShuffleHeader();
    header.readFields(input);
    input.close();

    shuffleHandler.stop();
    Assert.assertTrue("sendError called when client closed connection",
        failures.size() == 0);

    Assert.assertEquals("Should have no caught exceptions",
        new ArrayList<>(), failures);
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
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, TEST_EXECUTION.getKeepAliveTimeout());
    testKeepAliveInternal(conf, ShuffleUrlType.SIMPLE, ShuffleUrlType.WITH_KEEPALIVE);
  }

  //TODO snemeth implement keepalive test that used properly mocked ShuffleHandler
  @Test(timeout = 10000)
  public void testKeepAliveInitiallyDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, false);
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, TEST_EXECUTION.getKeepAliveTimeout());
    testKeepAliveInternal(conf, ShuffleUrlType.WITH_KEEPALIVE, ShuffleUrlType.WITH_KEEPALIVE);
  }
  private void testKeepAliveInternal(Configuration conf, ShuffleUrlType... shuffleUrlTypes) throws IOException {
    Assert.assertTrue("Expected at least two shuffle URL types ",
        shuffleUrlTypes.length >= 2);
    ShuffleHandlerForKeepAliveTests shuffleHandler = new ShuffleHandlerForKeepAliveTests(HEADER_WRITE_COUNT, ATTEMPT_ID);
    shuffleHandler.init(conf);
    shuffleHandler.start();

    String[] urls = new String[shuffleUrlTypes.length];
    for (int i = 0; i < shuffleUrlTypes.length; i++) {
      if (shuffleUrlTypes[i] == ShuffleUrlType.SIMPLE) {
        urls[i] = getShuffleUrl(shuffleHandler, ATTEMPT_ID, ATTEMPT_ID);
      } else if (shuffleUrlTypes[i] == ShuffleUrlType.WITH_KEEPALIVE) {
        urls[i] = getShuffleUrlWithKeepAlive(shuffleHandler, ATTEMPT_ID, ATTEMPT_ID);
      }
    }

    HttpConnectionHelper httpConnectionHelper = new HttpConnectionHelper(shuffleHandler.lastSocketAddress);
    httpConnectionHelper.connectToUrls(urls);

    httpConnectionHelper.validate(connData -> {
      HttpConnectionAssert.create(connData)
          .expectKeepAliveWithTimeout(TEST_EXECUTION.getKeepAliveTimeout())
          .expectResponseSize(shuffleHandler.expectedResponseSize);
    });
    HttpConnectionAssert.assertKeepAliveConnectionsAreSame(httpConnectionHelper);
    Assert.assertEquals("Unexpected failure", new ArrayList<>(), shuffleHandler.failures);
  }

  @Test(timeout = 10000)
  public void testSocketKeepAlive() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    // try setting to -ve keep alive timeout.
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, -100);
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
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);
      Assert.assertTrue("socket should be set KEEP_ALIVE",
          shuffleHandler.isSocketKeepAlive());
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
      shuffleHandler.stop();
    }
    //TODO snemeth HADOOP-15327: Add back this assertion when bug is determined and fixed.
    // See detailed notes in jira
//    Assert.assertEquals("Should have no caught exceptions",
//        new ArrayList<>(), shuffleHandler.failures);
  }

  /**
   * simulate a reducer that sends an invalid shuffle-header - sometimes a wrong
   * header_name and sometimes a wrong version
   * 
   * @throws Exception exception
   */
  @Test (timeout = 10000)
  public void testIncompatibleShuffleVersion() throws Exception {
    final int failureNum = 3;
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
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
      Assert.assertEquals(
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
    
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String mapId, int reduce,
              String jobId, String user) throws IOException {
            // Do nothing.
            return null;
          }
          @Override
          protected void populateHeaders(List<String> mapIds, String jobId,
              String user, int reduce, HttpRequest request,
              HttpResponse response, boolean keepAliveParam,
              Map<String, MapOutputInfo> infoMap) throws IOException {
            // Do nothing.
          }
          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri)
                  throws IOException {
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
    int connAttempts = 3;
    HttpURLConnection[] conns = new HttpURLConnection[connAttempts];

    for (int i = 0; i < connAttempts; i++) {
      String URLstring = "http://127.0.0.1:" 
           + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
           + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_"
           + i + "_0";
      URL url = new URL(URLstring);
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
        Assert.fail("Expected a numerical value for RETRY_AFTER header field");
      } catch (Exception e) {
        Assert.fail("Expected a IOException");
      }
      int statusCode = conn.getResponseCode();
      LOG.debug("Connection status code: {}", statusCode);
      mapOfConnections.putIfAbsent(statusCode, new ArrayList<>());
      List<HttpURLConnection> connectionList = mapOfConnections.get(statusCode);
      connectionList.add(conn);
    }

    Assert.assertEquals("Expected only HTTP 200 and HTTP 429 response codes",
        Sets.newHashSet(
            HttpURLConnection.HTTP_OK,
            ShuffleHandler.TOO_MANY_REQ_STATUS.code()),
        mapOfConnections.keySet());
    
    List<HttpURLConnection> successfulConnections =
        mapOfConnections.get(HttpURLConnection.HTTP_OK);
    Assert.assertEquals("Expected exactly two requests " +
            "with HTTP 200 OK response code",
        2, successfulConnections.size());

    //Ensure exactly one connection is HTTP 429 (TOO MANY REQUESTS)
    List<HttpURLConnection> closedConnections =
        mapOfConnections.get(ShuffleHandler.TOO_MANY_REQ_STATUS.code());
    Assert.assertEquals("Expected exactly one HTTP 429 (Too Many Requests) response code",
        1, closedConnections.size());

    // This connection should be closed because it to above the limit
    HttpURLConnection conn = closedConnections.get(0);
    int rc = conn.getResponseCode();
    Assert.assertEquals("Expected a HTTP 429 (Too Many Requests) response code",
        ShuffleHandler.TOO_MANY_REQ_STATUS.code(), rc);
    long backoff = Long.parseLong(
        conn.getHeaderField(ShuffleHandler.RETRY_AFTER_HEADER));
    Assert.assertTrue("The backoff value cannot be negative.", backoff > 0);

    shuffleHandler.stop();

    //It's okay to get a ClosedChannelException.
    //All other kinds of exceptions means something went wrong
    Assert.assertEquals("Should have no caught exceptions",
        new ArrayList<>(), failures.stream()
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
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
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
    List<File> fileMap = new ArrayList<File>();
    createShuffleHandlerFiles(ABS_LOG_DIR, user, appId.toString(), appAttemptId,
        conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {

          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri)
              throws IOException {
            // Do nothing.
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
    AuxiliaryLocalPathHandler pathHandler = new TestAuxiliaryLocalPathHandler();
    shuffleHandler.setUseOutboundExceptionHandler(true);
    shuffleHandler.setAuxiliaryLocalPathHandler(pathHandler);
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<JobTokenIdentifier>("identifier".getBytes(),
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
      byte[] byteArr = new byte[10000];
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        is.readFully(byteArr);
      } catch (EOFException e) {
        // ignore
      }
      // Retrieve file owner name
      FileInputStream is = new FileInputStream(fileMap.get(0));
      String owner = NativeIO.POSIX.getFstat(is.getFD()).getOwner();
      is.close();

      String message =
          "Owner '" + owner + "' for path " + fileMap.get(0).getAbsolutePath()
              + " did not match expected owner '" + user + "'";
      String receivedString = new String(byteArr);
      Assert.assertTrue(String.format("Received string '%s' should contain " +
          "message '%s'", receivedString, message),
          receivedString.contains(message));
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(ABS_LOG_DIR);
    }

    Assert.assertEquals("Should have no caught exceptions",
        new ArrayList<>(), failures);
  }

  private static void createShuffleHandlerFiles(File logDir, String user,
      String appId, String appAttemptId, Configuration conf,
      List<File> fileMap) throws IOException {
    String attemptDir =
        StringUtils.join(Path.SEPARATOR,
            new String[] { logDir.getAbsolutePath(),
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

  private static void
    createMapOutputFile(File mapOutputFile, Configuration conf)
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
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS,
        ABS_LOG_DIR.getAbsolutePath());
    // emulate aux services startup with recovery enabled
    shuffle.setRecoveryPath(new Path(tmpDir.toString()));
    tmpDir.mkdirs();
    try {
      shuffle.init(conf);
      shuffle.start();

      // setup a shuffle token for an application
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>(
          "identifier".getBytes(), "password".getBytes(), new Text(user),
          new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffle.initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
            outputBuffer.getLength())));

      // verify we are authorized to shuffle
      int rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setAuxiliaryLocalPathHandler(pathHandler);
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

      // shutdown app and verify access is lost
      shuffle.stopApplication(new ApplicationTerminationContext(appId));
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we still don't have access
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, rc);
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
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
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

      // setup a shuffle token for an application
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>(
          "identifier".getBytes(), "password".getBytes(), new Text(user),
          new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffle.initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
              outputBuffer.getLength())));

      // verify we are authorized to shuffle
      int rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setAuxiliaryLocalPathHandler(pathHandler);
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);
      Version version = Version.newInstance(1, 0);
      Assert.assertEquals(version, shuffle.getCurrentVersion());
    
      // emulate shuffle handler restart with compatible version
      Version version11 = Version.newInstance(1, 1);
      // update version info before close shuffle
      shuffle.storeVersion(version11);
      Assert.assertEquals(version11, shuffle.loadVersion());
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setAuxiliaryLocalPathHandler(pathHandler);
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();
      // shuffle version will be override by CURRENT_VERSION_INFO after restart
      // successfully.
      Assert.assertEquals(version, shuffle.loadVersion());
      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);
    
      // emulate shuffle handler restart with incompatible version
      Version version21 = Version.newInstance(2, 1);
      shuffle.storeVersion(version21);
      Assert.assertEquals(version21, shuffle.loadVersion());
      shuffle.close();
      shuffle = new ShuffleHandlerForTests();
      shuffle.setAuxiliaryLocalPathHandler(pathHandler);
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
    
      try {
        shuffle.start();
        Assert.fail("Incompatible version, should expect fail here.");
      } catch (ServiceStateException e) {
        Assert.assertTrue("Exception message mismatch", 
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
    final ArrayList<Throwable> failures = new ArrayList<Throwable>(1);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "simple");
    UserGroupInformation.setConfiguration(conf);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, ABS_LOG_DIR.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    String reducerId = "0";
    List<File> fileMap = new ArrayList<File>();
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
              HttpResponse response, URL requestUri) throws IOException {
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
          new Token<JobTokenIdentifier>("identifier".getBytes(),
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
      Assert.assertEquals("sendError called due to shuffle error",
          0, failures.size());
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(ABS_LOG_DIR);
    }
  }

  @Test(timeout = 4000)
  public void testSendMapCount() throws Exception {
    final List<ShuffleHandler.ReduceMapFileCount> listenerList =
        new ArrayList<ShuffleHandler.ReduceMapFileCount>();
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

    Assert.assertEquals("Should have no caught exceptions",
        new ArrayList<>(), sh.failures);
  }

  @Test(timeout = 10000)
  public void testIdleStateHandlingSpecifiedTimeout() throws Exception {
    int timeoutSeconds = 4;
    int expectedTimeoutSeconds = timeoutSeconds;
    testHandlingIdleState(timeoutSeconds, expectedTimeoutSeconds);
  }

  @Test(timeout = 10000)
  public void testIdleStateHandlingNegativeTimeoutDefaultsTo1Second() throws Exception {
    int timeoutSeconds = -100;
    int expectedTimeoutSeconds = 1;
    testHandlingIdleState(timeoutSeconds, expectedTimeoutSeconds);
  }

  private String getShuffleUrlWithKeepAlive(ShuffleHandler shuffleHandler, long jobId, long attemptId) {
    String url = getShuffleUrl(shuffleHandler, jobId, attemptId);
    return url + "&keepAlive=true";
  }

  private String getShuffleUrl(ShuffleHandler shuffleHandler, long jobId, long attemptId) {
    String port = shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
    String shuffleBaseURL = "http://127.0.0.1:" + port;
    String location = String.format("/mapOutput" +
        "?job=job_%s_1" +
        "&reduce=1" +
        "&map=attempt_%s_1_m_1_0", jobId, attemptId);
    return shuffleBaseURL + location;
  }

  private void testHandlingIdleState(int configuredTimeoutSeconds, int expectedTimeoutSeconds) throws IOException,
      InterruptedException {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, DEFAULT_PORT);
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, configuredTimeoutSeconds);

    final CountDownLatch countdownLatch = new CountDownLatch(1);
    ShuffleHandlerForKeepAliveTests shuffleHandler = new ShuffleHandlerForKeepAliveTests(HEADER_WRITE_COUNT, ATTEMPT_ID, event -> {
      countdownLatch.countDown();
    });
    shuffleHandler.init(conf);
    shuffleHandler.start();

    String shuffleUrl = getShuffleUrl(shuffleHandler, ATTEMPT_ID, ATTEMPT_ID);
    String[] urls = new String[] {shuffleUrl};
    HttpConnectionHelper httpConnectionHelper = new HttpConnectionHelper(shuffleHandler.lastSocketAddress);
    long beforeConnectionTimestamp = System.currentTimeMillis();
    httpConnectionHelper.connectToUrls(urls);
    countdownLatch.await();
    long channelClosedTimestamp = System.currentTimeMillis();
    long secondsPassed =
        TimeUnit.SECONDS.convert(channelClosedTimestamp - beforeConnectionTimestamp, TimeUnit.MILLISECONDS);
    Assert.assertTrue(String.format("Expected at least %s seconds of timeout. " +
            "Actual timeout seconds: %s", expectedTimeoutSeconds, secondsPassed),
        secondsPassed >= expectedTimeoutSeconds);
  }

  public ChannelFuture createMockChannelFuture(Channel mockCh,
      final List<ShuffleHandler.ReduceMapFileCount> listenerList) {
    final ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockFuture.channel()).thenReturn(mockCh);
    Mockito.doReturn(true).when(mockFuture).isSuccess();
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        //Add ReduceMapFileCount listener to a list
        if (invocation.getArguments()[0].getClass() ==
            ShuffleHandler.ReduceMapFileCount.class)
          listenerList.add((ShuffleHandler.ReduceMapFileCount)
              invocation.getArguments()[0]);
        return null;
      }
    }).when(mockFuture).addListener(Mockito.any(
        ShuffleHandler.ReduceMapFileCount.class));
    return mockFuture;
  }

  public HttpRequest createMockHttpRequest() {
    HttpRequest mockHttpRequest = mock(HttpRequest.class);
    Mockito.doReturn(HttpMethod.GET).when(mockHttpRequest).method();
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        String uri = "/mapOutput?job=job_12345_1&reduce=1";
        for (int i = 0; i < 100; i++)
          uri = uri.concat("&map=attempt_12345_1_m_" + i + "_0");
        return uri;
      }
    }).when(mockHttpRequest).uri();
    return mockHttpRequest;
  }
}
