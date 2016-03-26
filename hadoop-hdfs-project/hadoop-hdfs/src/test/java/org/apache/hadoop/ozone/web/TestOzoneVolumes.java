/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.web;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.web.exceptions.ErrorTable;
import org.apache.hadoop.ozone.web.headers.Header;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertEquals;

public class TestOzoneVolumes {
  static MiniDFSCluster cluster = null;
  static int port = 0;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting DFS_OBJECTSTORE_ENABLED_KEY = true and
   * DFS_STORAGE_HANDLER_TYPE_KEY = "local" , which uses a local directory to
   * emulate Ozone backend.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();

    URL p = conf.getClass().getResource("");
    String path = p.getPath().concat(TestOzoneVolumes.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT,
        OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT_DEFAULT);

    conf.set(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT, path);
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY, "local");
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_TRACE_ENABLED_KEY, true);
    Logger.getLogger("log4j.logger.org.apache.http").setLevel(Level.DEBUG);

    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    DataNode dataNode = cluster.getDataNodes().get(0);
    port = dataNode.getInfoPort();
  }

  /**
   * shutdown MiniDFSCluster
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Creates Volumes on Ozone Store.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumes() throws IOException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);
    HttpClient client = new DefaultHttpClient();
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    try {
      HttpPost httppost = new HttpPost(
          String.format("http://localhost:%d/%s", port, volumeName));

      httppost.addHeader(Header.OZONE_VERSION_HEADER,
          Header.OZONE_V1_VERSION_HEADER);
      httppost.addHeader(HttpHeaders.DATE,
          format.format(new Date(Time.monotonicNow())));
      httppost.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              OzoneConsts.OZONE_SIMPLE_HDFS_USER);
      httppost.addHeader(Header.OZONE_USER, OzoneConsts.OZONE_SIMPLE_HDFS_USER);

      HttpResponse response = client.execute(httppost);
      assertEquals(response.toString(), HTTP_CREATED,
          response.getStatusLine().getStatusCode());
    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  /**
   * Create Volumes with Quota.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesWithQuota() throws IOException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);
    HttpClient client = new DefaultHttpClient();
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    try {
      HttpPost httppost = new HttpPost(
          String.format("http://localhost:%d/%s?quota=10TB", port, volumeName));

      httppost.addHeader(Header.OZONE_VERSION_HEADER,
          Header.OZONE_V1_VERSION_HEADER);
      httppost.addHeader(HttpHeaders.DATE,
          format.format(new Date(Time.monotonicNow())));
      httppost.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              OzoneConsts.OZONE_SIMPLE_HDFS_USER);
      httppost.addHeader(Header.OZONE_USER, OzoneConsts.OZONE_SIMPLE_HDFS_USER);

      HttpResponse response = client.execute(httppost);
      assertEquals(response.toString(), HTTP_CREATED,
          response.getStatusLine().getStatusCode());
    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  /**
   * Create Volumes with Invalid Quota.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesWithInvalidQuota() throws IOException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);
    HttpClient client = new DefaultHttpClient();
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    try {
      HttpPost httppost = new HttpPost(
          String.format("http://localhost:%d/%s?quota=NaN", port, volumeName));

      httppost.addHeader(Header.OZONE_VERSION_HEADER,
          Header.OZONE_V1_VERSION_HEADER);
      httppost.addHeader(HttpHeaders.DATE,
          format.format(new Date(Time.monotonicNow())));
      httppost.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              OzoneConsts.OZONE_SIMPLE_HDFS_USER);
      httppost.addHeader(Header.OZONE_USER, OzoneConsts.OZONE_SIMPLE_HDFS_USER);

      HttpResponse response = client.execute(httppost);
      assertEquals(response.toString(), ErrorTable.MALFORMED_QUOTA
              .getHttpCode(),
          response.getStatusLine().getStatusCode());
    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  /**
   * To create a volume a user name must be specified using OZONE_USER header.
   * This test verifies that we get an error in case we call without a OZONE
   * user name.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesWithInvalidUser() throws IOException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);
    HttpClient client = new DefaultHttpClient();
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    try {
      HttpPost httppost = new HttpPost(
          String.format("http://localhost:%d/%s?quota=1TB", port, volumeName));

      httppost.addHeader(Header.OZONE_VERSION_HEADER,
          Header.OZONE_V1_VERSION_HEADER);
      httppost.addHeader(HttpHeaders.DATE,
          format.format(new Date(Time.monotonicNow())));
      httppost.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              OzoneConsts.OZONE_SIMPLE_HDFS_USER);

      HttpResponse response = client.execute(httppost);

      assertEquals(response.toString(), ErrorTable.USER_NOT_FOUND.getHttpCode(),
          response.getStatusLine().getStatusCode());
    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  /**
   * Only Admins can create volumes in Ozone. This test uses simple userauth as
   * backend and hdfs and root are admin users in the simple backend.
   * <p>
   * This test tries to create a volume as user bilbo.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesWithOutAdminRights() throws IOException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);
    HttpClient client = new DefaultHttpClient();
    String volumeName = OzoneUtils.getRequestID().toLowerCase();
    try {
      HttpPost httppost = new HttpPost(
          String.format("http://localhost:%d/%s?quota=NaN", port, volumeName));

      httppost.addHeader(Header.OZONE_VERSION_HEADER,
          Header.OZONE_V1_VERSION_HEADER);
      httppost.addHeader(HttpHeaders.DATE,
          format.format(new Date(Time.monotonicNow())));
      httppost.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              "bilbo"); // This is not a root user in Simple Auth
      httppost.addHeader(Header.OZONE_USER, OzoneConsts.OZONE_SIMPLE_HDFS_USER);

      HttpResponse response = client.execute(httppost);
      assertEquals(response.toString(), ErrorTable.ACCESS_DENIED.getHttpCode(),
          response.getStatusLine().getStatusCode());
    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  /**
   * Create a bunch of volumes in a loop.
   *
   * @throws IOException
   */
  @Test
  public void testCreateVolumesInLoop() throws IOException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);

    for (int x = 0; x < 1000; x++) {
      HttpClient client = new DefaultHttpClient();
      String volumeName = OzoneUtils.getRequestID().toLowerCase();
      String userName = OzoneUtils.getRequestID().toLowerCase();

      HttpPost httppost = new HttpPost(
          String.format("http://localhost:%d/%s?quota=10TB", port, volumeName));

      httppost.addHeader(Header.OZONE_VERSION_HEADER,
          Header.OZONE_V1_VERSION_HEADER);
      httppost.addHeader(HttpHeaders.DATE,
          format.format(new Date(Time.monotonicNow())));
      httppost.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              OzoneConsts.OZONE_SIMPLE_HDFS_USER);
      httppost.addHeader(Header.OZONE_USER, userName);

      HttpResponse response = client.execute(httppost);
      assertEquals(response.toString(), HTTP_CREATED,
          response.getStatusLine().getStatusCode());
      client.getConnectionManager().shutdown();
    }
  }
  /**
   * Get volumes owned by the user.
   *
   * @throws IOException
   */
  @Test
  public void testGetVolumesByUser() throws IOException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);
    // We need to create a volume for this test to succeed.
    testCreateVolumes();
    HttpClient client = new DefaultHttpClient();
    try {
      HttpGet httpget =
          new HttpGet(String.format("http://localhost:%d/", port));

      httpget.addHeader(Header.OZONE_VERSION_HEADER,
          Header.OZONE_V1_VERSION_HEADER);

      httpget.addHeader(HttpHeaders.DATE,
          format.format(new Date(Time.monotonicNow())));

      httpget.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              OzoneConsts.OZONE_SIMPLE_HDFS_USER);

      httpget.addHeader(Header.OZONE_USER,
          OzoneConsts.OZONE_SIMPLE_HDFS_USER );

      HttpResponse response = client.execute(httpget);
      assertEquals(response.toString(), HTTP_OK,
          response.getStatusLine().getStatusCode());

    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  /**
   * Admins can read volumes belonging to other users.
   *
   * @throws IOException
   */
  @Test
  public void testGetVolumesOfAnotherUser() throws IOException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);

    HttpClient client = new DefaultHttpClient();
    try {
      HttpGet httpget =
          new HttpGet(String.format("http://localhost:%d/", port));

      httpget.addHeader(Header.OZONE_VERSION_HEADER,
          Header.OZONE_V1_VERSION_HEADER);
      httpget.addHeader(HttpHeaders.DATE,
          format.format(new Date(Time.monotonicNow())));

      httpget.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              OzoneConsts.OZONE_SIMPLE_ROOT_USER);

      // User Root is getting volumes belonging to user HDFS
      httpget.addHeader(Header.OZONE_USER, OzoneConsts.OZONE_SIMPLE_HDFS_USER);

      HttpResponse response = client.execute(httpget);
      assertEquals(response.toString(), HTTP_OK,
          response.getStatusLine().getStatusCode());

    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  /**
   * if you try to read volumes belonging to another user,
   * then server always ignores it.
   *
   * @throws IOException
   */
  @Test
  public void testGetVolumesOfAnotherUserShouldFail() throws IOException {
    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss ZZZ", Locale.US);

    HttpClient client = new DefaultHttpClient();
    String userName = OzoneUtils.getRequestID().toLowerCase();
    try {
      HttpGet httpget =
          new HttpGet(String.format("http://localhost:%d/", port));

      httpget.addHeader(Header.OZONE_VERSION_HEADER,
          Header.OZONE_V1_VERSION_HEADER);
      httpget.addHeader(HttpHeaders.DATE,
          format.format(new Date(Time.monotonicNow())));

      httpget.addHeader(HttpHeaders.AUTHORIZATION,
          Header.OZONE_SIMPLE_AUTHENTICATION_SCHEME + " " +
              userName);

      // userName is NOT a root user, hence he should NOT be able to read the
      // volumes of user HDFS
      httpget.addHeader(Header.OZONE_USER, OzoneConsts.OZONE_SIMPLE_HDFS_USER);

      HttpResponse response = client.execute(httpget);
      // We will get an Error called userNotFound when using Simple Auth Scheme
      assertEquals(response.toString(), ErrorTable.USER_NOT_FOUND.getHttpCode(),
          response.getStatusLine().getStatusCode());

    } finally {
      client.getConnectionManager().shutdown();
    }
  }

}
