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
package org.apache.hadoop.crypto.key.kms.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URL;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.ThreadUtil;

public class MiniKMS {

  public static class Builder {
    private File kmsConfDir;
    private String log4jConfFile;
    private File keyStoreFile;
    private String keyStorePassword;
    private int inPort;

    public Builder() {
      kmsConfDir = new File("target/test-classes").getAbsoluteFile();
      log4jConfFile = "kms-log4j.properties";
    }

    public Builder setKmsConfDir(File confDir) {
      Preconditions.checkNotNull(confDir, "KMS conf dir is NULL");
      Preconditions.checkArgument(confDir.exists(),
          "KMS conf dir does not exist");
      kmsConfDir = confDir;
      return this;
    }

    public Builder setLog4jConfFile(String log4jConfFile) {
      Preconditions.checkNotNull(log4jConfFile, "log4jconf file is NULL");
      this.log4jConfFile = log4jConfFile;
      return this;
    }

    public Builder setPort(int port) {
      Preconditions.checkArgument(port > 0, "input port must be greater than 0");
      this.inPort = port;
      return this;
    }

    public Builder setSslConf(File keyStoreFile, String keyStorePassword) {
      Preconditions.checkNotNull(keyStoreFile, "keystore file is NULL");
      Preconditions.checkNotNull(keyStorePassword, "keystore password is NULL");
      Preconditions.checkArgument(keyStoreFile.exists(),
          "keystore file does not exist");
      this.keyStoreFile = keyStoreFile;
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    public MiniKMS build() {
      Preconditions.checkArgument(kmsConfDir.exists(),
          "KMS conf dir does not exist");
      return new MiniKMS(kmsConfDir.getAbsolutePath(), log4jConfFile,
          (keyStoreFile != null) ? keyStoreFile.getAbsolutePath() : null,
          keyStorePassword, inPort);
    }
  }

  private String kmsConfDir;
  private String log4jConfFile;
  private String keyStore;
  private String keyStorePassword;
  private KMSWebServer jetty;
  private int inPort;
  private URL kmsURL;

  public MiniKMS(String kmsConfDir, String log4ConfFile, String keyStore,
      String password, int inPort) {
    this.kmsConfDir = kmsConfDir;
    this.log4jConfFile = log4ConfFile;
    this.keyStore = keyStore;
    this.keyStorePassword = password;
    this.inPort = inPort;
  }

  private void copyResource(String inputResourceName, File outputFile) throws
      IOException {
    InputStream is = null;
    OutputStream os = null;
    try {
      is = ThreadUtil.getResourceAsStream(inputResourceName);
      os = new FileOutputStream(outputFile);
      IOUtils.copy(is, os);
    } finally {
      IOUtils.closeQuietly(is);
      IOUtils.closeQuietly(os);
    }
  }

  public void start() throws Exception {
    System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, kmsConfDir);
    File aclsFile = new File(kmsConfDir, "kms-acls.xml");
    if (!aclsFile.exists()) {
      copyResource("mini-kms-acls-default.xml", aclsFile);
    }
    File coreFile = new File(kmsConfDir, "core-site.xml");
    if (!coreFile.exists()) {
      Configuration core = new Configuration();
      Writer writer = new FileWriter(coreFile);
      core.writeXml(writer);
      writer.close();
    }
    File kmsFile = new File(kmsConfDir, "kms-site.xml");
    if (!kmsFile.exists()) {
      Configuration kms = new Configuration(false);
      kms.set(KMSConfiguration.KEY_PROVIDER_URI,
          "jceks://file@" + new Path(kmsConfDir, "kms.keystore").toUri());
      kms.set("hadoop.kms.authentication.type", "simple");
      Writer writer = new FileWriter(kmsFile);
      kms.writeXml(writer);
      writer.close();
    }
    System.setProperty("log4j.configuration", log4jConfFile);

    final Configuration conf = KMSConfiguration.getKMSConf();
    conf.set(KMSConfiguration.HTTP_HOST_KEY, "localhost");
    conf.setInt(KMSConfiguration.HTTP_PORT_KEY, inPort);
    if (keyStore != null) {
      conf.setBoolean(KMSConfiguration.SSL_ENABLED_KEY, true);
      conf.set(SSLFactory.SSL_SERVER_KEYSTORE_LOCATION, keyStore);
      conf.set(SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD, keyStorePassword);
      conf.set(SSLFactory.SSL_SERVER_KEYSTORE_TYPE, "jks");
    }

    jetty = new KMSWebServer(conf);
    jetty.start();
    kmsURL = jetty.getKMSUrl();
  }

  public URL getKMSUrl() {
    return kmsURL;
  }

  public void stop() {
    if (jetty != null && jetty.isRunning()) {
      try {
        jetty.stop();
        jetty = null;
      } catch (Exception ex) {
        throw new RuntimeException("Could not stop MiniKMS embedded Jetty, " +
            ex.getMessage(), ex);
      }
    }
  }

}
