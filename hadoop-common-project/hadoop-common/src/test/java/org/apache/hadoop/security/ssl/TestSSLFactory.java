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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import java.io.File;
import java.net.URL;
import java.security.GeneralSecurityException;

public class TestSSLFactory {

  private static final String BASEDIR =
    System.getProperty("test.build.dir", "target/test-dir") + "/" +
    TestSSLFactory.class.getSimpleName();

  @BeforeClass
  public static void setUp() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
  }

  private Configuration createConfiguration(boolean clientCert)
    throws Exception {
    Configuration conf = new Configuration();
    String keystoresDir = new File(BASEDIR).getAbsolutePath();
    String sslConfsDir = KeyStoreTestUtil.getClasspathDir(TestSSLFactory.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfsDir, conf, clientCert);
    return conf;
  }

  @After
  @Before
  public void cleanUp() throws Exception {
    String keystoresDir = new File(BASEDIR).getAbsolutePath();
    String sslConfsDir = KeyStoreTestUtil.getClasspathDir(TestSSLFactory.class);
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfsDir);
  }

  @Test(expected = IllegalStateException.class)
  public void clientMode() throws Exception {
    Configuration conf = createConfiguration(false);
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    try {
      sslFactory.init();
      Assert.assertNotNull(sslFactory.createSSLSocketFactory());
      Assert.assertNotNull(sslFactory.getHostnameVerifier());
      sslFactory.createSSLServerSocketFactory();
    } finally {
      sslFactory.destroy();
    }
  }

  private void serverMode(boolean clientCert, boolean socket) throws Exception {
    Configuration conf = createConfiguration(clientCert);
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
    try {
      sslFactory.init();
      Assert.assertNotNull(sslFactory.createSSLServerSocketFactory());
      Assert.assertEquals(clientCert, sslFactory.isClientCertRequired());
      if (socket) {
        sslFactory.createSSLSocketFactory();
      } else {
        sslFactory.getHostnameVerifier();
      }
    } finally {
      sslFactory.destroy();
    }
  }


  @Test(expected = IllegalStateException.class)
  public void serverModeWithoutClientCertsSocket() throws Exception {
    serverMode(false, true);
  }

  @Test(expected = IllegalStateException.class)
  public void serverModeWithClientCertsSocket() throws Exception {
    serverMode(true, true);
  }

  @Test(expected = IllegalStateException.class)
  public void serverModeWithoutClientCertsVerifier() throws Exception {
    serverMode(false, false);
  }

  @Test(expected = IllegalStateException.class)
  public void serverModeWithClientCertsVerifier() throws Exception {
    serverMode(true, false);
  }

  @Test
  public void validHostnameVerifier() throws Exception {
    Configuration conf = createConfiguration(false);
    conf.unset(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY);
    SSLFactory sslFactory = new
      SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("DEFAULT", sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("ALLOW_ALL",
                        sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "DEFAULT_AND_LOCALHOST");
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("DEFAULT_AND_LOCALHOST",
                        sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "STRICT");
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("STRICT", sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();

    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "STRICT_IE6");
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    Assert.assertEquals("STRICT_IE6",
                        sslFactory.getHostnameVerifier().toString());
    sslFactory.destroy();
  }

  @Test(expected = GeneralSecurityException.class)
  public void invalidHostnameVerifier() throws Exception {
    Configuration conf = createConfiguration(false);
    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "foo");
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    try {
      sslFactory.init();
    } finally {
      sslFactory.destroy();
    }
  }

  @Test
  public void testConnectionConfigurator() throws Exception {
    Configuration conf = createConfiguration(false);
    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "STRICT_IE6");
    SSLFactory sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    try {
      sslFactory.init();
      HttpsURLConnection sslConn =
          (HttpsURLConnection) new URL("https://foo").openConnection();
      Assert.assertNotSame("STRICT_IE6",
                           sslConn.getHostnameVerifier().toString());
      sslFactory.configure(sslConn);
      Assert.assertEquals("STRICT_IE6",
                          sslConn.getHostnameVerifier().toString());
    } finally {
      sslFactory.destroy();
    }
  }

}
