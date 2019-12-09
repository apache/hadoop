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

package org.apache.hadoop.yarn.applications.unmanagedamlauncher;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUnmanagedAMLauncher {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestUnmanagedAMLauncher.class);

  protected static MiniYARNCluster yarnCluster = null;
  protected static Configuration conf = new YarnConfiguration();

  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    LOG.info("Starting up YARN cluster");
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    if (yarnCluster == null) {
      yarnCluster = new MiniYARNCluster(
          TestUnmanagedAMLauncher.class.getSimpleName(), 1, 1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      //get the address
      Configuration yarnClusterConfig = yarnCluster.getConfig();
      LOG.info("MiniYARN ResourceManager published address: " +
               yarnClusterConfig.get(YarnConfiguration.RM_ADDRESS));
      LOG.info("MiniYARN ResourceManager published web address: " +
               yarnClusterConfig.get(YarnConfiguration.RM_WEBAPP_ADDRESS));
      String webapp = yarnClusterConfig.get(YarnConfiguration.RM_WEBAPP_ADDRESS);
      assertTrue("Web app address still unbound to a host at " + webapp,
        !webapp.startsWith("0.0.0.0"));
      LOG.info("Yarn webapp is at "+ webapp);
      URL url = Thread.currentThread().getContextClassLoader()
          .getResource("yarn-site.xml");
      if (url == null) {
        throw new RuntimeException(
            "Could not find 'yarn-site.xml' dummy file in classpath");
      }
      //write the document to a buffer (not directly to the file, as that
      //can cause the file being written to get read -which will then fail.
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      yarnClusterConfig.writeXml(bytesOut);
      bytesOut.close();
      //write the bytes to the file in the classpath
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      os.write(bytesOut.toByteArray());
      os.close();
    }
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (yarnCluster != null) {
      try {
        yarnCluster.stop();
      } finally {
        yarnCluster = null;
      }
    }
  }

  private static String getTestRuntimeClasspath() {
    LOG.info("Trying to generate classpath for app master from current thread's classpath");
    String envClassPath = "";
    String cp = System.getProperty("java.class.path");
    if (cp != null) {
      envClassPath += cp.trim() + File.pathSeparator;
    }
    // yarn-site.xml at this location contains proper config for mini cluster
    ClassLoader thisClassLoader = Thread.currentThread()
      .getContextClassLoader();
    URL url = thisClassLoader.getResource("yarn-site.xml");
    envClassPath += new File(url.getFile()).getParent();
    return envClassPath;
  }

  @Test(timeout=30000)
  public void testUMALauncher() throws Exception {
    String classpath = getTestRuntimeClasspath();
    String javaHome = System.getenv("JAVA_HOME");
    if (javaHome == null) {
      LOG.error("JAVA_HOME not defined. Test not running.");
      return;
    }
    String[] args = {
        "--classpath",
        classpath,
        "--queue",
        "default",
        "--cmd",
        javaHome
            + "/bin/java -Xmx512m "
            + TestUnmanagedAMLauncher.class.getCanonicalName()
            + " success" };

    LOG.info("Initializing Launcher");
    UnmanagedAMLauncher launcher =
        new UnmanagedAMLauncher(new Configuration(yarnCluster.getConfig())) {
          public void launchAM(ApplicationAttemptId attemptId)
              throws IOException, YarnException {
            YarnApplicationAttemptState attemptState =
                rmClient.getApplicationAttemptReport(attemptId)
                  .getYarnApplicationAttemptState();
            Assert.assertTrue(attemptState
              .equals(YarnApplicationAttemptState.LAUNCHED));
            super.launchAM(attemptId);
          }
        };
    boolean initSuccess = launcher.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running Launcher");
    boolean result = launcher.run();

    LOG.info("Launcher run completed. Result=" + result);
    Assert.assertTrue(result);

  }

  @Test(timeout=30000)
  public void testUMALauncherError() throws Exception {
    String classpath = getTestRuntimeClasspath();
    String javaHome = System.getenv("JAVA_HOME");
    if (javaHome == null) {
      LOG.error("JAVA_HOME not defined. Test not running.");
      return;
    }
    String[] args = {
        "--classpath",
        classpath,
        "--queue",
        "default",
        "--cmd",
        javaHome
            + "/bin/java -Xmx512m "
            + TestUnmanagedAMLauncher.class.getCanonicalName()
            + " failure" };

    LOG.info("Initializing Launcher");
    UnmanagedAMLauncher launcher = new UnmanagedAMLauncher(new Configuration(
        yarnCluster.getConfig()));
    boolean initSuccess = launcher.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running Launcher");

    try {
      launcher.run();
      fail("Expected an exception to occur as launch should have failed");
    } catch (RuntimeException e) {
      // Expected
    }
  }

  // provide main method so this class can act as AM
  public static void main(String[] args) throws Exception {
    if (args[0].equals("success")) {
      ApplicationMasterProtocol client = ClientRMProxy.createRMProxy(conf,
          ApplicationMasterProtocol.class);
      client.registerApplicationMaster(RegisterApplicationMasterRequest
          .newInstance(NetUtils.getHostname(), -1, ""));
      Thread.sleep(1000);
      FinishApplicationMasterResponse resp =
          client.finishApplicationMaster(FinishApplicationMasterRequest
            .newInstance(FinalApplicationStatus.SUCCEEDED, "success", null));
      assertTrue(resp.getIsUnregistered());
      System.exit(0);
    } else {
      System.exit(1);
    }
  }
}
