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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.NodeLabelTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestConfigurationNodeLabelsProvider extends NodeLabelTestBase {

  protected static File testRootDir = new File("target",
      TestConfigurationNodeLabelsProvider.class.getName() + "-localDir")
      .getAbsoluteFile();

  final static File nodeLabelsConfigFile = new File(testRootDir,
      "yarn-site.xml");

  private static XMLPathClassLoader loader;

  private ConfigurationNodeLabelsProvider nodeLabelsProvider;

  @Before
  public void setup() {
    loader =
        new XMLPathClassLoader(
            TestConfigurationNodeLabelsProvider.class.getClassLoader());
    testRootDir.mkdirs();

    nodeLabelsProvider = new ConfigurationNodeLabelsProvider();
  }

  @After
  public void tearDown() throws Exception {
    if (nodeLabelsProvider != null) {
      nodeLabelsProvider.close();
    }
    if (testRootDir.exists()) {
      FileContext.getLocalFSFileContext().delete(
          new Path(testRootDir.getAbsolutePath()), true);
    }
  }

  private Configuration getConfForNodeLabels() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_LABELS, "A,B,CX");
    return conf;
  }

  @Test
  public void testNodeLabelsFromConfig() throws IOException,
      InterruptedException {
    Configuration conf = getConfForNodeLabels();
    nodeLabelsProvider.init(conf);
    // test for ensuring labels are set during initialization of the class
    nodeLabelsProvider.start();
    Thread.sleep(1000l); // sleep so that timer has run once during
                         // initialization
    assertNLCollectionEquals(toNodeLabelSet("A", "B", "CX"),
        nodeLabelsProvider.getNodeLabels());

    // test for valid Modification
    TimerTask timerTask = nodeLabelsProvider.getTimerTask();
    modifyConfAndCallTimer(timerTask, "X,y,Z");
    assertNLCollectionEquals(toNodeLabelSet("X", "y", "Z"),
        nodeLabelsProvider.getNodeLabels());
  }

  @Test
  public void testConfigForNoTimer() throws Exception {
    Configuration conf = getConfForNodeLabels();
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

  private static void modifyConfAndCallTimer(TimerTask timerTask,
      String nodeLabels) throws FileNotFoundException, IOException {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_LABELS, nodeLabels);
    conf.writeXml(new FileOutputStream(nodeLabelsConfigFile));
    ClassLoader actualLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      timerTask.run();
    } finally {
      Thread.currentThread().setContextClassLoader(actualLoader);
    }
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
