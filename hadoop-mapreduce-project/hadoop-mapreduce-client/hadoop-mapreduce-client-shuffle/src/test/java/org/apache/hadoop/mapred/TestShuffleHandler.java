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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.apache.hadoop.test.MockitoMaker.make;
import static org.apache.hadoop.test.MockitoMaker.stub;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.records.Version;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.jetty.HttpHeaders;

public class TestShuffleHandler {
  static final long MiB = 1024 * 1024; 
  private static final Log LOG = LogFactory.getLog(TestShuffleHandler.class);

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
    ShuffleHandler sh = new ShuffleHandler(ms);
    ChannelFuture cf = make(stub(ChannelFuture.class).
        returning(true, false).from.isSuccess());

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
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String base, String mapId,
              int reduce, String user) throws IOException {
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
            // send a shuffle header and a lot of data down the channel
            // to trigger a broken pipe
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i = 0; i < 100000; ++i) {
              header.write(dob);
            }
            return ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.getChannel().close();
            }
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.getChannel().close();
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
    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    DataInputStream input = new DataInputStream(conn.getInputStream());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    Assert.assertEquals("close", conn.getHeaderField(HttpHeaders.CONNECTION));
    ShuffleHeader header = new ShuffleHeader();
    header.readFields(input);
    input.close();

    shuffleHandler.stop();
    Assert.assertTrue("sendError called when client closed connection",
        failures.size() == 0);
  }

  @Test(timeout = 10000)
  public void testKeepAlive() throws Exception {
    final ArrayList<Throwable> failures = new ArrayList<Throwable>(1);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    // try setting to -ve keep alive timeout.
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, -100);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(final Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String base, String mapId,
              int reduce, String user) throws IOException {
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
            // Send some dummy data (populate content length details)
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            dob = new DataOutputBuffer();
            for (int i = 0; i < 100000; ++i) {
              header.write(dob);
            }

            long contentLength = dob.getLength();
            // for testing purpose;
            // disable connectinKeepAliveEnabled if keepAliveParam is available
            if (keepAliveParam) {
              connectionKeepAliveEnabled = false;
            }

            super.setResponseHeaders(response, keepAliveParam, contentLength);
          }

          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
              Channel ch, String user, String mapId, int reduce,
              MapOutputInfo info) throws IOException {
            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

            // send a shuffle header and a lot of data down the channel
            // to trigger a broken pipe
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i = 0; i < 100000; ++i) {
              header.write(dob);
            }
            return ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }

          @Override
          protected void sendError(ChannelHandlerContext ctx,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.getChannel().close();
            }
          }

          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.getChannel().close();
            }
          }
        };
      }
    };
    shuffleHandler.init(conf);
    shuffleHandler.start();

    String shuffleBaseURL = "http://127.0.0.1:"
            + shuffleHandler.getConfig().get(
              ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
    URL url =
        new URL(shuffleBaseURL + "/mapOutput?job=job_12345_1&reduce=1&"
            + "map=attempt_12345_1_m_1_0");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
      ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
      ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    DataInputStream input = new DataInputStream(conn.getInputStream());
    Assert.assertEquals(HttpHeaders.KEEP_ALIVE,
      conn.getHeaderField(HttpHeaders.CONNECTION));
    Assert.assertEquals("timeout=1",
      conn.getHeaderField(HttpHeaders.KEEP_ALIVE));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    ShuffleHeader header = new ShuffleHeader();
    header.readFields(input);
    input.close();

    // For keepAlive via URL
    url =
        new URL(shuffleBaseURL + "/mapOutput?job=job_12345_1&reduce=1&"
            + "map=attempt_12345_1_m_1_0&keepAlive=true");
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
      ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
      ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    input = new DataInputStream(conn.getInputStream());
    Assert.assertEquals(HttpHeaders.KEEP_ALIVE,
      conn.getHeaderField(HttpHeaders.CONNECTION));
    Assert.assertEquals("timeout=1",
      conn.getHeaderField(HttpHeaders.KEEP_ALIVE));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    header = new ShuffleHeader();
    header.readFields(input);
    input.close();
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
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    ShuffleHandler shuffleHandler = new ShuffleHandler();
    shuffleHandler.init(conf);
    shuffleHandler.start();

    // simulate a reducer that closes early by reading a single shuffle header
    // then closing the connection
    URL url = new URL("http://127.0.0.1:"
      + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
      + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_1_0");
    for (int i = 0; i < failureNum; ++i) {
      HttpURLConnection conn = (HttpURLConnection)url.openConnection();
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
    
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String base, String mapId,
              int reduce, String user) throws IOException {
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
            ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i=0; i<100000; ++i) {
              header.write(dob);
            }
            return ch.write(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }
        };
      }
    };
    shuffleHandler.init(conf);
    shuffleHandler.start();

    // setup connections
    int connAttempts = 3;
    HttpURLConnection conns[] = new HttpURLConnection[connAttempts];

    for (int i = 0; i < connAttempts; i++) {
      String URLstring = "http://127.0.0.1:" 
           + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
           + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_"
           + i + "_0";
      URL url = new URL(URLstring);
      conns[i] = (HttpURLConnection)url.openConnection();
      conns[i].setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conns[i].setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    }

    // Try to open numerous connections
    for (int i = 0; i < connAttempts; i++) {
      conns[i].connect();
    }

    //Ensure first connections are okay
    conns[0].getInputStream();
    int rc = conns[0].getResponseCode();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);
    
    conns[1].getInputStream();
    rc = conns[1].getResponseCode();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

    // This connection should be closed because it to above the limit
    try {
      conns[2].getInputStream();
      rc = conns[2].getResponseCode();
      Assert.fail("Expected a SocketException");
    } catch (SocketException se) {
      LOG.info("Expected - connection should not be open");
    } catch (Exception e) {
      Assert.fail("Expected a SocketException");
    }
    
    shuffleHandler.stop(); 
  }

  /**
   * Validate the ownership of the map-output files being pulled in. The
   * local-file-system owner of the file should match the user component in the
   *
   * @throws Exception exception
   */
  @Test(timeout = 100000)
  public void testMapFileAccess() throws IOException {
    // This will run only in NativeIO is enabled as SecureIOUtils need it
    assumeTrue(NativeIO.isAvailable());
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    File absLogDir = new File("target",
        TestShuffleHandler.class.getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absLogDir.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    LOG.info(appId.toString());
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    String reducerId = "0";
    List<File> fileMap = new ArrayList<File>();
    createShuffleHandlerFiles(absLogDir, user, appId.toString(), appAttemptId,
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

        };
      }
    };
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
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
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
      Assert.assertTrue((new String(byteArr)).contains(message));
    } finally {
      shuffleHandler.stop();
    }
  }

  private static void createShuffleHandlerFiles(File logDir, String user,
      String appId, String appAttemptId, Configuration conf,
      List<File> fileMap) throws IOException {
    String attemptDir =
        StringUtils.join(Path.SEPARATOR,
            Arrays.asList(new String[] { logDir.getAbsolutePath(),
                ContainerLocalizer.USERCACHE, user,
                ContainerLocalizer.APPCACHE, appId, "output", appAttemptId }));
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
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    ShuffleHandler shuffle = new ShuffleHandler();
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
      shuffle = new ShuffleHandler();
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
      shuffle = new ShuffleHandler();
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
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    ShuffleHandler shuffle = new ShuffleHandler();
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
      shuffle = new ShuffleHandler();
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
      shuffle = new ShuffleHandler();
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
      shuffle = new ShuffleHandler();
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
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
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
}
