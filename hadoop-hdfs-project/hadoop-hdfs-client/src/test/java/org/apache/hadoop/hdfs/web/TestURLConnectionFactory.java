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
package org.apache.hadoop.hdfs.web;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import static org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory.SSL_MONITORING_THREAD_NAME;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.slf4j.LoggerFactory;

public final class TestURLConnectionFactory {

  @Test
  public void testConnConfiguratior() throws IOException {
    final URL u = new URL("http://localhost");
    final List<HttpURLConnection> conns = Lists.newArrayList();
    URLConnectionFactory fc = new URLConnectionFactory(new ConnectionConfigurator() {
      @Override
      public HttpURLConnection configure(HttpURLConnection conn)
          throws IOException {
        Assert.assertEquals(u, conn.getURL());
        conns.add(conn);
        return conn;
      }
    });

    fc.openConnection(u);
    Assert.assertEquals(1, conns.size());
  }

  @Test
  public void testSSLInitFailure() throws Exception {
    Configuration conf = new Configuration();
    conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "foo");
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(
            LoggerFactory.getLogger(URLConnectionFactory.class));
    URLConnectionFactory.newDefaultURLConnectionFactory(conf);
    Assert.assertTrue("Expected log for ssl init failure not found!",
        logs.getOutput().contains(
        "Cannot load customized ssl related configuration"));
  }

  @Test
  public void testSSLFactoryCleanup() throws Exception {
    String baseDir = GenericTestUtils.getTempPath(
        TestURLConnectionFactory.class.getSimpleName());
    File base = new File(baseDir);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    String keystoreDir = new File(baseDir).getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(
        TestURLConnectionFactory.class);
    Configuration conf = new Configuration();
    KeyStoreTestUtil.setupSSLConfig(keystoreDir, sslConfDir, conf, false,
        true);
    Configuration sslConf = KeyStoreTestUtil.getSslConfig();

    sslConf.set("fs.defaultFS", "swebhdfs://localhost");
    FileSystem fs = FileSystem.get(sslConf);

    ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();

    while (threadGroup.getParent() != null) {
      threadGroup = threadGroup.getParent();
    }

    Thread[] threads = new Thread[threadGroup.activeCount()];

    threadGroup.enumerate(threads);
    Thread reloaderThread = null;
    for (Thread thread : threads) {
      if ((thread.getName() != null)
          && (thread.getName().contains(SSL_MONITORING_THREAD_NAME))) {
        reloaderThread = thread;
      }
    }
    Assert.assertTrue("Reloader is not alive", reloaderThread.isAlive());

    fs.close();

    boolean reloaderStillAlive = true;
    for (int i = 0; i < 10; i++) {
      reloaderStillAlive = reloaderThread.isAlive();
      if (!reloaderStillAlive) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertFalse("Reloader is still alive", reloaderStillAlive);
  }
}
