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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ssl.SslSelectChannelConnectorSecure;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.security.SslSelectChannelConnector;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.UUID;

public class MiniKMS {

  private static Server createJettyServer(String keyStore, String password, int inPort) {
    try {
      boolean ssl = keyStore != null;
      InetAddress localhost = InetAddress.getByName("localhost");
      String host = "localhost";
      ServerSocket ss = new ServerSocket((inPort < 0) ? 0 : inPort, 50, localhost);
      int port = ss.getLocalPort();
      ss.close();
      Server server = new Server(0);
      if (!ssl) {
        server.getConnectors()[0].setHost(host);
        server.getConnectors()[0].setPort(port);
      } else {
        SslSelectChannelConnector c = new SslSelectChannelConnectorSecure();
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
        == SslSelectChannelConnectorSecure.class;
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
    private int inPort = -1;

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
  private Server jetty;
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

  public void start() throws Exception {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, kmsConfDir);
    File aclsFile = new File(kmsConfDir, "kms-acls.xml");
    if (!aclsFile.exists()) {
      InputStream is = cl.getResourceAsStream("mini-kms-acls-default.xml");
      OutputStream os = new FileOutputStream(aclsFile);
      IOUtils.copy(is, os);
      is.close();
      os.close();
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
    jetty = createJettyServer(keyStore, keyStorePassword, inPort);

    // we need to do a special handling for MiniKMS to work when in a dir and
    // when in a JAR in the classpath thanks to Jetty way of handling of webapps
    // when they are in the a DIR, WAR or JAR.
    URL webXmlUrl = cl.getResource("kms-webapp/WEB-INF/web.xml");
    if (webXmlUrl == null) {
      throw new RuntimeException(
          "Could not find kms-webapp/ dir in test classpath");
    }
    boolean webXmlInJar = webXmlUrl.getPath().contains(".jar!/");
    String webappPath;
    if (webXmlInJar) {
      File webInf = new File("target/" + UUID.randomUUID().toString() +
          "/kms-webapp/WEB-INF");
      webInf.mkdirs();
      new File(webInf, "web.xml").delete();
      InputStream is = cl.getResourceAsStream("kms-webapp/WEB-INF/web.xml");
      OutputStream os = new FileOutputStream(new File(webInf, "web.xml"));
      IOUtils.copy(is, os);
      is.close();
      os.close();
      webappPath = webInf.getParentFile().getAbsolutePath();
    } else {
      webappPath = cl.getResource("kms-webapp").getPath();
    }
    WebAppContext context = new WebAppContext(webappPath, "/kms");
    if (webXmlInJar) {
      context.setClassLoader(cl);
    }
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
