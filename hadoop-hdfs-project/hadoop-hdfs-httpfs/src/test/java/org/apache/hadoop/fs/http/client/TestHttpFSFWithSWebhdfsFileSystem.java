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

package org.apache.hadoop.fs.http.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.TestJettyHelper;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.UUID;

@RunWith(value = Parameterized.class)
public class TestHttpFSFWithSWebhdfsFileSystem
  extends TestHttpFSWithHttpFSFileSystem {
  private static String classpathDir;
  private static final String BASEDIR = System.getProperty("test.build.dir",
      "target/test-dir") + "/" + UUID.randomUUID();

  private static Configuration sslConf;

  {
    URL url = Thread.currentThread().getContextClassLoader().
        getResource("classutils.txt");
    classpathDir = url.toExternalForm();
    if (classpathDir.startsWith("file:")) {
      classpathDir = classpathDir.substring("file:".length());
      classpathDir = classpathDir.substring(0,
          classpathDir.length() - "/classutils.txt".length());
    } else {
      throw new RuntimeException("Cannot find test classes dir");
    }
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    String keyStoreDir = new File(BASEDIR).getAbsolutePath();
    try {
      sslConf = new Configuration();
      KeyStoreTestUtil.setupSSLConfig(keyStoreDir, classpathDir, sslConf, false);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    jettyTestHelper = new TestJettyHelper("jks", keyStoreDir + "/serverKS.jks",
        "serverP");
  }

  @AfterClass
  public static void cleanUp() {
    new File(classpathDir, "ssl-client.xml").delete();
    new File(classpathDir, "ssl-server.xml").delete();
  }

  public TestHttpFSFWithSWebhdfsFileSystem(Operation operation) {
    super(operation);
  }

  @Override
  protected Class getFileSystemClass() {
    return SWebHdfsFileSystem.class;
  }

  @Override
  protected String getScheme() {
    return "swebhdfs";
  }

  @Override
  protected FileSystem getHttpFSFileSystem() throws Exception {
    Configuration conf = new Configuration(sslConf);
    conf.set("fs.swebhdfs.impl", getFileSystemClass().getName());
    URI uri = new URI("swebhdfs://" +
        TestJettyHelper.getJettyURL().toURI().getAuthority());
    return FileSystem.get(uri, conf);
  }

}
