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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.createTrustStore;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.generateCertificate;
import static org.apache.hadoop.security.ssl.KeyStoreTestUtil.generateKeyPair;

public class TestReloadingX509TrustManager {

  private static final String BASEDIR =
    System.getProperty("test.build.data", "target/test-dir") + "/" +
    TestReloadingX509TrustManager.class.getSimpleName();

  private X509Certificate cert1;
  private X509Certificate cert2;

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
      new ReloadingX509TrustManager("jks", truststoreLocation, "password", 10);
    try {
      tm.init();
    } finally {
      tm.destroy();
    }
  }

  @Test(expected = IOException.class)
  public void testLoadCorruptTrustStore() throws Exception {
    String truststoreLocation = BASEDIR + "/testcorrupt.jks";
    OutputStream os = new FileOutputStream(truststoreLocation);
    os.write(1);
    os.close();

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", truststoreLocation, "password", 10);
    try {
      tm.init();
    } finally {
      tm.destroy();
    }
  }

  @Test
  public void testReload() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    String truststoreLocation = BASEDIR + "/testreload.jks";
    createTrustStore(truststoreLocation, "password", "cert1", cert1);

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", truststoreLocation, "password", 10);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);

      // Wait so that the file modification time is different
      Thread.sleep((tm.getReloadInterval() + 1000));

      // Add another cert
      Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
      certs.put("cert1", cert1);
      certs.put("cert2", cert2);
      createTrustStore(truststoreLocation, "password", certs);

      // and wait to be sure reload has taken place
      assertEquals(10, tm.getReloadInterval());

      // Wait so that the file modification time is different
      Thread.sleep((tm.getReloadInterval() + 200));

      assertEquals(2, tm.getAcceptedIssuers().length);
    } finally {
      tm.destroy();
    }
  }

  @Test
  public void testReloadMissingTrustStore() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    String truststoreLocation = BASEDIR + "/testmissing.jks";
    createTrustStore(truststoreLocation, "password", "cert1", cert1);

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", truststoreLocation, "password", 10);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);
      X509Certificate cert = tm.getAcceptedIssuers()[0];
      new File(truststoreLocation).delete();

      // Wait so that the file modification time is different
      Thread.sleep((tm.getReloadInterval() + 200));

      assertEquals(1, tm.getAcceptedIssuers().length);
      assertEquals(cert, tm.getAcceptedIssuers()[0]);
    } finally {
      tm.destroy();
    }
  }

  @Test
  public void testReloadCorruptTrustStore() throws Exception {
    KeyPair kp = generateKeyPair("RSA");
    cert1 = generateCertificate("CN=Cert1", kp, 30, "SHA1withRSA");
    cert2 = generateCertificate("CN=Cert2", kp, 30, "SHA1withRSA");
    String truststoreLocation = BASEDIR + "/testcorrupt.jks";
    createTrustStore(truststoreLocation, "password", "cert1", cert1);

    ReloadingX509TrustManager tm =
      new ReloadingX509TrustManager("jks", truststoreLocation, "password", 10);
    try {
      tm.init();
      assertEquals(1, tm.getAcceptedIssuers().length);
      X509Certificate cert = tm.getAcceptedIssuers()[0];

      OutputStream os = new FileOutputStream(truststoreLocation);
      os.write(1);
      os.close();
      new File(truststoreLocation).setLastModified(System.currentTimeMillis() -
                                                   1000);

      // Wait so that the file modification time is different
      Thread.sleep((tm.getReloadInterval() + 200));

      assertEquals(1, tm.getAcceptedIssuers().length);
      assertEquals(cert, tm.getAcceptedIssuers()[0]);
    } finally {
      tm.destroy();
    }
  }

}
