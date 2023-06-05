/**
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
package org.apache.hadoop.security.ssl;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;

import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.createTrustStore;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.generateCertificate;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.generateKeyPair;
import static org.junit.Assert.assertFalse;

public class TestReloadingX509TrustManager {

  private static final String BASEDIR = GenericTestUtils.getTempPath(
      TestReloadingX509TrustManager.class.getSimpleName());

  private X509Certificate cert1;
  private X509Certificate cert2;
  private final LogCapturer reloaderLog = LogCapturer.captureLogs(
      FileMonitoringTimerTask.LOG);

  @BeforeClass
  public static void setUp() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
  }

  @Test(expected = IOException.class)
  public void testLoadMissingTrustStore() throws Exception {
    String truststoreLocation = BASEDIR + "/testmissing.jks";

    ReloadingX509TrustManager tm =
            new ReloadingX509TrustManager("jks", truststoreLocation, "password");
  }

  @Test(expected = IOException.class)
  public void testLoadCorruptTrustStore() throws Exception {
    String truststoreLocation = BASEDIR + "/testcorrupt.jks";
    OutputStream os = new FileOutputStream(truststoreLocation);
    os.write(1);
    os.close();

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", truststoreLocation, "password");
  }

  @Test (timeout = 30000)
  public void testReload() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    String truststoreLocation = BASEDIR + "/testreload.jks";
    createTrustStore(truststoreLocation, "password", "cert1", cert1);

    long reloadInterval = 10;
    Timer fileMonitoringTimer = new Timer(FileBasedKeyStoresFactory.SSL_MONITORING_THREAD_NAME, true);
    final ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", truststoreLocation, "password");
    try {
      fileMonitoringTimer.schedule(new FileMonitoringTimerTask(
              Paths.get(truststoreLocation), tm::loadFrom,null), reloadInterval, reloadInterval);
      assertEquals(1, tm.getAcceptedIssuers().length);

      // Wait so that the file modification time is different
      Thread.sleep((reloadInterval+ 1000));

      // Add another cert
      Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
      certs.put("cert1", cert1);
      certs.put("cert2", cert2);
      createTrustStore(truststoreLocation, "password", certs);

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return tm.getAcceptedIssuers().length == 2;
        }
      }, (int) reloadInterval, 100000);
    } finally {
      fileMonitoringTimer.cancel();
    }
  }

  @Test (timeout = 30000)
  public void testReloadMissingTrustStore() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    String truststoreLocation = BASEDIR + "/testmissing.jks";
    createTrustStore(truststoreLocation, "password", "cert1", cert1);

    long reloadInterval = 10;
    Timer fileMonitoringTimer = new Timer(FileBasedKeyStoresFactory.SSL_MONITORING_THREAD_NAME, true);
    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", truststoreLocation, "password");
    try {
      fileMonitoringTimer.schedule(new FileMonitoringTimerTask(
              Paths.get(truststoreLocation), tm::loadFrom,null), reloadInterval, reloadInterval);
      assertEquals(1, tm.getAcceptedIssuers().length);
      X509Certificate cert = tm.getAcceptedIssuers()[0];

      assertFalse(reloaderLog.getOutput().contains(
              FileMonitoringTimerTask.PROCESS_ERROR_MESSAGE));

      // Wait for the first reload to happen so we actually detect a change after the delete
      Thread.sleep((reloadInterval+ 1000));

      new File(truststoreLocation).delete();

      // Wait for the reload to happen and log to get written to
      Thread.sleep((reloadInterval+ 1000));

      waitForFailedReloadAtLeastOnce((int) reloadInterval);

      assertEquals(1, tm.getAcceptedIssuers().length);
      assertEquals(cert, tm.getAcceptedIssuers()[0]);
    } finally {
      reloaderLog.stopCapturing();
      fileMonitoringTimer.cancel();
    }
  }


  @Test (timeout = 30000)
  public void testReloadCorruptTrustStore() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    String truststoreLocation = BASEDIR + "/testcorrupt.jks";
    createTrustStore(truststoreLocation, "password", "cert1", cert1);

    long reloadInterval = 10;
    Timer fileMonitoringTimer = new Timer(FileBasedKeyStoresFactory.SSL_MONITORING_THREAD_NAME, true);
    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", truststoreLocation, "password");
    try {
      fileMonitoringTimer.schedule(new FileMonitoringTimerTask(
              Paths.get(truststoreLocation), tm::loadFrom,null), reloadInterval, reloadInterval);
      assertEquals(1, tm.getAcceptedIssuers().length);
      final X509Certificate cert = tm.getAcceptedIssuers()[0];

      // Wait so that the file modification time is different
      Thread.sleep((reloadInterval + 1000));

      assertFalse(reloaderLog.getOutput().contains(
              FileMonitoringTimerTask.PROCESS_ERROR_MESSAGE));
      OutputStream os = new FileOutputStream(truststoreLocation);
      os.write(1);
      os.close();

      waitForFailedReloadAtLeastOnce((int) reloadInterval);

      assertEquals(1, tm.getAcceptedIssuers().length);
      assertEquals(cert, tm.getAcceptedIssuers()[0]);
    } finally {
      reloaderLog.stopCapturing();
      fileMonitoringTimer.cancel();
    }
  }

  /**Wait for the reloader thread to load the configurations at least once
   * by probing the log of the thread if the reload fails.
   */
  private void waitForFailedReloadAtLeastOnce(int reloadInterval)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return reloaderLog.getOutput().contains(
            FileMonitoringTimerTask.PROCESS_ERROR_MESSAGE);
      }
    }, reloadInterval, 10 * 1000);
  }

  /** No password when accessing a trust store is legal. */
  @Test
  public void testNoPassword() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    String truststoreLocation = BASEDIR + "/testreload.jks";
    createTrustStore(truststoreLocation, "password", "cert1", cert1);

    Timer fileMonitoringTimer = new Timer(FileBasedKeyStoresFactory.SSL_MONITORING_THREAD_NAME, true);
    final ReloadingX509TrustManager tm =
        new ReloadingX509TrustManager("jks", truststoreLocation, null);
    try {
      fileMonitoringTimer.schedule(new FileMonitoringTimerTask(
              Paths.get(truststoreLocation), tm::loadFrom,null), 10, 10);
      assertEquals(1, tm.getAcceptedIssuers().length);
    } finally {
      fileMonitoringTimer.cancel();
    }
  }
}
