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
package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.TimerTask;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestConfigurationNodeLabelsProvider extends NodeLabelTestBase {

  protected static File testRootDir = new File("target",
      TestConfigurationNodeLabelsProvider.class.getName() + "-localDir")
      .getAbsoluteFile();

  final static File nodeLabelsConfigFile = new File(testRootDir,
      "yarn-site.xml");

  private static XMLPathClassLoader loader;

  private ConfigurationNodeLabelsProvider nodeLabelsProvider;

  @BeforeClass
  public static void create() {
    loader =
        new XMLPathClassLoader(
            TestConfigurationNodeLabelsProvider.class.getClassLoader());
    testRootDir.mkdirs();
    Thread.currentThread().setContextClassLoader(loader);
  }

  @Before
  public void setup() {
    nodeLabelsProvider = new ConfigurationNodeLabelsProvider();
  }

  @After
  public void tearDown() throws Exception {
    if (nodeLabelsProvider != null) {
      nodeLabelsProvider.close();
      nodeLabelsProvider.stop();
    }
  }

  @AfterClass
  public static void remove() throws Exception {
    if (testRootDir.exists()) {
      FileContext.getLocalFSFileContext().delete(
          new Path(testRootDir.getAbsolutePath()), true);
    }
  }

  @Test
  public void testNodeLabelsFromConfig() throws IOException,
      InterruptedException {
    Configuration conf = new Configuration();
    modifyConf("A,B,CX");
    nodeLabelsProvider.init(conf);
    // test for ensuring labels are set during initialization of the class
    nodeLabelsProvider.start();
    assertNLCollectionEquals(toNodeLabelSet("A", "B", "CX"),
        nodeLabelsProvider.getNodeLabels());

    // test for valid Modification
    TimerTask timerTask = nodeLabelsProvider.getTimerTask();
    modifyConf("X,y,Z");
    timerTask.run();
    assertNLCollectionEquals(toNodeLabelSet("X", "y", "Z"),
        nodeLabelsProvider.getNodeLabels());
  }

  @Test
  public void testConfigForNoTimer() throws Exception {
    Configuration conf = new Configuration();
    modifyConf("A,B,CX");
    conf.setLong(YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        AbstractNodeLabelsProvider.DISABLE_NODE_LABELS_PROVIDER_FETCH_TIMER);
    nodeLabelsProvider.init(conf);
    nodeLabelsProvider.start();
    Assert
        .assertNull(
            "Timer is not expected to be created when interval is configured as -1",
            nodeLabelsProvider.nodeLabelsScheduler);
    // Ensure that even though timer is not run, node labels are fetched at least once so
    // that NM registers/updates Labels with RM
    assertNLCollectionEquals(toNodeLabelSet("A", "B", "CX"),
        nodeLabelsProvider.getNodeLabels());
  }

  @Test
  public void testConfigTimer() throws Exception {
    Configuration conf = new Configuration();
    modifyConf("A,B,CX");
    conf.setLong(YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        1000);
    nodeLabelsProvider.init(conf);
    nodeLabelsProvider.start();
    // Ensure that even though timer is not run, node labels are fetched at
    // least once so
    // that NM registers/updates Labels with RM
    assertNLCollectionEquals(toNodeLabelSet("A", "B", "CX"),
        nodeLabelsProvider.getNodeLabels());
    modifyConf("X,y,Z");
    Thread.sleep(1500);
    assertNLCollectionEquals(toNodeLabelSet("X", "y", "Z"),
        nodeLabelsProvider.getNodeLabels());

  }

  private static void modifyConf(String nodeLabels)
      throws FileNotFoundException, IOException {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_LABELS, nodeLabels);
    FileOutputStream confStream = new FileOutputStream(nodeLabelsConfigFile);
    conf.writeXml(confStream);
    IOUtils.closeQuietly(confStream);
  }

  private static class XMLPathClassLoader extends ClassLoader {
    public XMLPathClassLoader(ClassLoader wrapper) {
      super(wrapper);
    }

    public URL getResource(String name) {
      if (name.equals(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE)) {
        try {
          return nodeLabelsConfigFile.toURI().toURL();
        } catch (MalformedURLException e) {
          e.printStackTrace();
          Assert.fail();
        }
      }
      return super.getResource(name);
    }
  }
}
