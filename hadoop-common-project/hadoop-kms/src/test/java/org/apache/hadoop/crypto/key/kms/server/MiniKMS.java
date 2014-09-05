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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class MiniKMS {

  private static Server createJettyServer(String keyStore, String password) {
    try {
      boolean ssl = keyStore != null;
      InetAddress localhost = InetAddress.getByName("localhost");
      String host = "localhost";
      ServerSocket ss = new ServerSocket(0, 50, localhost);
      int port = ss.getLocalPort();
      ss.close();
      Server server = new Server(0);
      if (!ssl) {
        server.getConnectors()[0].setHost(host);
        server.getConnectors()[0].setPort(port);
      } else {
        SslSocketConnector c = new SslSocketConnector();
        c.setHost(host);
        c.setPort(port);
        c.setNeedClientAuth(false);
        c.setKeystore(keyStore);
        c.setKeystoreType("jks");
        c.setKeyPassword(password);
        server.setConnectors(new Connector[]{c});
      }
      return server;
    } catch (Exception ex) {
      throw new RuntimeException("Could not start embedded servlet container, "
          + ex.getMessage(), ex);
    }
  }

  private static URL getJettyURL(Server server) {
    boolean ssl = server.getConnectors()[0].getClass()
        == SslSocketConnector.class;
    try {
      String scheme = (ssl) ? "https" : "http";
      return new URL(scheme + "://" +
          server.getConnectors()[0].getHost() + ":" +
          server.getConnectors()[0].getPort());
    } catch (MalformedURLException ex) {
      throw new RuntimeException("It should never happen, " + ex.getMessage(),
          ex);
    }
  }

  public static class Builder {
    private File kmsConfDir;
    private String log4jConfFile;
    private File keyStoreFile;
    private String keyStorePassword;

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
          keyStorePassword);
    }
  }

  private String kmsConfDir;
  private String log4jConfFile;
  private String keyStore;
  private String keyStorePassword;
  private Server jetty;
  private URL kmsURL;

  public MiniKMS(String kmsConfDir, String log4ConfFile, String keyStore,
      String password) {
    this.kmsConfDir = kmsConfDir;
    this.log4jConfFile = log4ConfFile;
    this.keyStore = keyStore;
    this.keyStorePassword = password;
  }

  public void start() throws Exception {
    System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, kmsConfDir);
    File aclsFile = new File(kmsConfDir, "kms-acls.xml");
    if (!aclsFile.exists()) {
      Configuration acls = new Configuration(false);
      Writer writer = new FileWriter(aclsFile);
      acls.writeXml(writer);
      writer.close();
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
      kms.set("hadoop.security.key.provider.path",
          "jceks://file@" + kmsConfDir + "/kms.keystore");
      kms.set("hadoop.kms.authentication.type", "simple");
      Writer writer = new FileWriter(kmsFile);
      kms.writeXml(writer);
      writer.close();
    }
    System.setProperty("log4j.configuration", log4jConfFile);
    jetty = createJettyServer(keyStore, keyStorePassword);
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL url = cl.getResource("kms-webapp");
    if (url == null) {
      throw new RuntimeException(
          "Could not find kms-webapp/ dir in test classpath");
    }
    WebAppContext context = new WebAppContext(url.getPath(), "/kms");
    jetty.addHandler(context);
    jetty.start();
    kmsURL = new URL(getJettyURL(jetty), "kms");
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
