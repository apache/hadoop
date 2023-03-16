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

import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.HttpResponseStatus;

import static org.apache.hadoop.mapred.ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY;
import static org.apache.hadoop.mapreduce.security.SecureShuffleUtils.HTTP_HEADER_URL_HASH;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.nativeio.NativeIO;
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
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.records.Version;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestShuffleHandler extends TestShuffleHandlerBase {
  static final long MIB = 1024 * 1024;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestShuffleHandler.class);

  private static final HttpResponseStatus OK_STATUS = new HttpResponseStatus(200, "OK");
  private static final ApplicationId TEST_APP_ID = ApplicationId.newInstance(1111111111111L, 1);

  /**
   * Test the validation of ShuffleHandler's meta-data's serialization and
   * de-serialization.
   *
   * @throws Exception exception
   */
  @Test(timeout = 10000)
  public void testSerializeMeta() throws Exception {
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
  @Test(timeout = 10000)
  public void testShuffleMetrics() throws Exception {
    MetricsSystem ms = new MetricsSystemImpl();
    ShuffleHandler sh = new ShuffleHandler(ms);
    ChannelFuture cf = mock(ChannelFuture.class);
    when(cf.isSuccess()).thenReturn(true).thenReturn(false);

    sh.metrics.shuffleConnections.incr();
    sh.metrics.shuffleOutputBytes.incr(MIB);
    sh.metrics.shuffleConnections.incr();
    sh.metrics.shuffleOutputBytes.incr(2 * MIB);

    checkShuffleMetrics(ms, 3 * MIB, 0, 0, 2);

    sh.metrics.operationComplete(cf);
    sh.metrics.operationComplete(cf);

    checkShuffleMetrics(ms, 3 * MIB, 1, 1, 0);

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
   * Validate the limit on number of shuffle connections.
   *
   * @throws Exception exception
   */
  @Test(timeout = 10000)
  public void testMaxConnections() throws Exception {
    final int maxAllowedConnections = 3;
    final int notAcceptedConnections = 1;
    final int connAttempts = maxAllowedConnections + notAcceptedConnections;

    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, maxAllowedConnections);
    ShuffleHandlerMock shuffleHandler = new ShuffleHandlerMock();
    shuffleHandler.init(conf);
    shuffleHandler.start();
    final String port = shuffleHandler.getConfig().get(SHUFFLE_PORT_CONFIG_KEY);
    final SecretKey secretKey = shuffleHandler.addTestApp(TEST_USER);

    // setup connections
    HttpURLConnection[] conns = new HttpURLConnection[connAttempts];

    for (int i = 0; i < connAttempts; i++) {
      conns[i] = createRequest(
          geURL(port, TEST_JOB_ID, 0, Collections.singletonList(TEST_ATTEMPT_1), true),
          secretKey);
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
  }

  /**
   * Validate the limit on number of shuffle connections.
   *
   * @throws Exception exception
   */
  @Test(timeout = 10000)
  public void testKeepAlive() throws Exception {
    Configuration conf = new Configuration();
    ShuffleHandlerMock shuffleHandler = new ShuffleHandlerMock();
    shuffleHandler.init(conf);
    shuffleHandler.start();
    final String port = shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
    final SecretKey secretKey = shuffleHandler.addTestApp(TEST_USER);

    HttpURLConnection conn1 = createRequest(
        geURL(port, TEST_JOB_ID, 0, Collections.singletonList(TEST_ATTEMPT_1), true),
        secretKey);
    conn1.connect();
    verifyContent(conn1, TEST_DATA_A);

    HttpURLConnection conn2 = createRequest(
        geURL(port, TEST_JOB_ID, 0, Collections.singletonList(TEST_ATTEMPT_2), true),
        secretKey);
    conn2.connect();
    verifyContent(conn2, TEST_DATA_B);

    HttpURLConnection conn3 = createRequest(
        geURL(port, TEST_JOB_ID, 0, Collections.singletonList(TEST_ATTEMPT_3), false),
        secretKey);
    conn3.connect();
    verifyContent(conn3, TEST_DATA_C);

    shuffleHandler.stop();

    List<String> actual = matchLogs("connections=\\d+");
    assertEquals("only one connection was used",
        Arrays.asList("connections=1", "connections=0"), actual);
  }

  /**
   * Validate the ownership of the map-output files being pulled in. The
   * local-file-system owner of the file should match the user component in the
   *
   * @throws IOException exception
   */
  @Test(timeout = 100000)
  public void testMapFileAccess() throws IOException {
    // This will run only in NativeIO is enabled as SecureIOUtils need it
    assumeTrue(NativeIO.isAvailable());
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);

    final String randomUser = "randomUser";
    final String attempt = "attempt_1111111111111_0004_m_000004_0";
    generateMapOutput(randomUser, tempDir.toAbsolutePath().toString(), attempt,
            Arrays.asList(TEST_DATA_C, TEST_DATA_B, TEST_DATA_A));

    ShuffleHandlerMock shuffleHandler = new ShuffleHandlerMock();
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      final String port = shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
      final SecretKey secretKey = shuffleHandler.addTestApp(randomUser);

      HttpURLConnection conn = createRequest(
          geURL(port, TEST_JOB_ID, 0, Collections.singletonList(attempt), false),
          secretKey);
      conn.connect();

      InputStream is = null;
      try {
        is = conn.getInputStream();
      } catch (IOException ioe) {
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
          is = conn.getErrorStream();
        }
      }

      assertNotNull(is);
      BufferedReader in = new BufferedReader(new InputStreamReader(is));
      StringBuilder builder = new StringBuilder();
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        System.out.println(inputLine);
        builder.append(inputLine);
      }
      String receivedString = builder.toString();

      //Retrieve file owner name
      String indexFilePath = getIndexFile(randomUser, tempDir.toAbsolutePath().toString(), attempt);
      String owner;
      try (FileInputStream fis = new FileInputStream(indexFilePath)) {
        owner = NativeIO.POSIX.getFstat(fis.getFD()).getOwner();
      }

      String message =
          "Owner '" + owner + "' for path " + indexFilePath
              + " did not match expected owner '" + randomUser + "'";
      assertTrue(String.format("Received string '%s' should contain " +
              "message '%s'", receivedString, message),
          receivedString.contains(message));
      assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, conn.getResponseCode());
      LOG.info("received: " + receivedString);
      assertNotEquals("", receivedString);
    } finally {
      shuffleHandler.stop();
    }
  }

  @Test
  public void testRecovery() throws IOException {
    final File tmpDir = new File(System.getProperty("test.build.data",
        System.getProperty("java.io.tmpdir")),
        TestShuffleHandler.class.getName());
    ShuffleHandlerMock shuffle = new ShuffleHandlerMock();
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    // emulate aux services startup with recovery enabled
    shuffle.setRecoveryPath(new Path(tmpDir.toString()));
    assertTrue(tmpDir.mkdirs());
    try {
      shuffle.init(conf);
      shuffle.start();
      final String port = shuffle.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
      final SecretKey secretKey = shuffle.addTestApp(TEST_USER);

      // verify we are authorized to shuffle
      int rc = getShuffleResponseCode(port, secretKey);
      assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandlerMock();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(port, secretKey);
      assertEquals(HttpURLConnection.HTTP_OK, rc);

      // shutdown app and verify access is lost
      shuffle.stopApplication(new ApplicationTerminationContext(TEST_APP_ID));
      rc = getShuffleResponseCode(port, secretKey);
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandlerMock();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we still don't have access
      rc = getShuffleResponseCode(port, secretKey);
      assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, rc);
    } finally {
      shuffle.close();
      FileUtil.fullyDelete(tmpDir);
    }
  }

  @Test
  public void testRecoveryFromOtherVersions() throws IOException {
    final File tmpDir = new File(System.getProperty("test.build.data",
        System.getProperty("java.io.tmpdir")),
        TestShuffleHandler.class.getName());
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    ShuffleHandlerMock shuffle = new ShuffleHandlerMock();
    // emulate aux services startup with recovery enabled
    shuffle.setRecoveryPath(new Path(tmpDir.toString()));
    assertTrue(tmpDir.mkdirs());
    try {
      shuffle.init(conf);
      shuffle.start();
      final String port = shuffle.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
      final SecretKey secretKey = shuffle.addTestApp(TEST_USER);

      // verify we are authorized to shuffle
      int rc = getShuffleResponseCode(port, secretKey);
      assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandlerMock();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(port, secretKey);
      assertEquals(HttpURLConnection.HTTP_OK, rc);
      Version version = Version.newInstance(1, 0);
      assertEquals(version, shuffle.getCurrentVersion());

      // emulate shuffle handler restart with compatible version
      Version version11 = Version.newInstance(1, 1);
      // update version info before close shuffle
      shuffle.storeVersion(version11);
      assertEquals(version11, shuffle.loadVersion());
      shuffle.close();
      shuffle = new ShuffleHandlerMock();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();
      // shuffle version will be override by CURRENT_VERSION_INFO after restart
      // successfully.
      assertEquals(version, shuffle.loadVersion());
      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(port, secretKey);
      assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart with incompatible version
      Version version21 = Version.newInstance(2, 1);
      shuffle.storeVersion(version21);
      assertEquals(version21, shuffle.loadVersion());
      shuffle.close();
      shuffle = new ShuffleHandlerMock();
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
      shuffle.close();
      FileUtil.fullyDelete(tmpDir);
    }
  }

  private static void verifyContent(HttpURLConnection conn,
                                    String expectedContent) throws IOException {
    DataInputStream input = new DataInputStream(conn.getInputStream());
    ShuffleHeader header = new ShuffleHeader();
    header.readFields(input);
    byte[] data = new byte[expectedContent.length()];
    assertEquals(expectedContent.length(), input.read(data));
    assertEquals(expectedContent, new String(data));
  }

  private static int getShuffleResponseCode(String port, SecretKey key) throws IOException {
    HttpURLConnection conn = createRequest(
        geURL(port, TEST_JOB_ID, 0, Collections.singletonList(TEST_ATTEMPT_1), false),
        key);
    conn.connect();
    int rc = conn.getResponseCode();
    conn.disconnect();
    return rc;
  }

  private static URL geURL(String port, String jobId, int reduce, List<String> maps,
                           boolean keepAlive) throws MalformedURLException {
    return new URL(getURLString(port, getUri(jobId, reduce, maps, keepAlive)));
  }

  private static String getURLString(String port, String uri) {
    return String.format("http://127.0.0.1:%s%s", port, uri);
  }

  private static HttpURLConnection createRequest(URL url, SecretKey secretKey) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    connection.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    String msgToEncode = SecureShuffleUtils.buildMsgFrom(url);
    connection.setRequestProperty(HTTP_HEADER_URL_HASH,
        SecureShuffleUtils.hashFromString(msgToEncode, secretKey));
    return connection;
  }

  class ShuffleHandlerMock extends ShuffleHandler {

    public SecretKey addTestApp(String user) throws IOException {
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt = new Token<>(
          "identifier".getBytes(), "password".getBytes(), new Text(user),
          new Text("shuffleService"));
      jt.write(outputBuffer);
      initializeApplication(new ApplicationInitializationContext(user, TEST_APP_ID,
          ByteBuffer.wrap(outputBuffer.getData(), 0,
              outputBuffer.getLength())));

      return JobTokenSecretManager.createSecretKey(jt.getPassword());
    }

    @Override
    protected ShuffleChannelHandlerContext createHandlerContext() {
      return new ShuffleChannelHandlerContext(getConfig(),
          userRsrc,
          secretManager,
          createLoadingCache(),
          new IndexCache(new JobConf(getConfig())),
          ms.register(new ShuffleHandler.ShuffleMetrics()),
          allChannels
      );
    }
  }
}