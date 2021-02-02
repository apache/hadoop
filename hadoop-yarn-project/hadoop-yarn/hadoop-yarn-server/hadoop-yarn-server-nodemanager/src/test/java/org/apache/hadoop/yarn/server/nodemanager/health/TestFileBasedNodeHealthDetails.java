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

package org.apache.hadoop.yarn.server.nodemanager.health;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.NodeHealthDetails;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class TestFileBasedNodeHealthDetails {

  private static final File TEST_ROOT_DIR = new File("target",
      TestFileBasedNodeHealthDetails.class.getName() + "-localDir")
      .getAbsoluteFile();
  private File nodeHealthDetailsFile = new File(TEST_ROOT_DIR,
      "nodeScore.xml");

  private NodeHealthDetailsReporter nodeHealthDetailsReporter;
  private Configuration conf;
  @Before
  public void setup() throws IOException {
    writeNodeHealthScoreFile();
    conf = new Configuration();
    conf.set(YarnConfiguration.NM_HEALTH_CHECK_SCORE_FILE,
        nodeHealthDetailsFile.getAbsolutePath());
    nodeHealthDetailsReporter = new FileBasedNodeHealthDetails(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (TEST_ROOT_DIR.exists()) {
      FileContext.getLocalFSFileContext().delete(
          new Path(TEST_ROOT_DIR.getAbsolutePath()), true);
    }
  }

  private void writeNodeHealthScoreFile() throws IOException {
    String xmlData = "<configuration>"
        + "<property><name>resource1</name><value>35</value></property>"
        + "<property><name>resource2</name><value>65</value></property>"
        + "</configuration>";
    FileUtils.writeStringToFile(nodeHealthDetailsFile, xmlData,
        StandardCharsets.UTF_8);
  }



  /**
   * Test NodeHealthDetails when its configured. The resourceScore file is
   * {@link #nodeHealthDetailsFile}
   */
  @Test
  public void testUpdateNodeHealthDetails() {

    HashMap<String, Integer> resourcesScrore = new HashMap<>();
    resourcesScrore.put("resource1", 35);
    resourcesScrore.put("resource2", 65);
    NodeHealthDetails nodeHealthDetails = NodeHealthDetails
        .newInstance(100, resourcesScrore);

    nodeHealthDetailsReporter.updateNodeHealthDetails();

    assertEquals(100, (int) nodeHealthDetailsReporter.getNodeHealthDetails()
        .getOverallScore());
    assertEquals(nodeHealthDetailsReporter.getNodeHealthDetails().toString(),
        nodeHealthDetails.toString());

    nodeHealthDetailsReporter.updateNodeHealthDetails();
    nodeHealthDetailsReporter.updateNodeHealthDetails();

    // The value should not be changed.
    assertEquals(100, (int) nodeHealthDetailsReporter.getNodeHealthDetails()
        .getOverallScore());
    assertEquals(nodeHealthDetailsReporter.getNodeHealthDetails().toString(),
        nodeHealthDetails.toString());

  }

  @Test
  public void testNodeHealthDetailsWhenDisabled() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_HEALTH_CHECK_SCORE_FILE, "");
    nodeHealthDetailsReporter = new FileBasedNodeHealthDetails(conf);

    // Testing with the default conf. The score should return 0.
    nodeHealthDetailsReporter.updateNodeHealthDetails();

    assertEquals(0, (int) nodeHealthDetailsReporter.getNodeHealthDetails()
        .getOverallScore());
    assertEquals(nodeHealthDetailsReporter.getNodeHealthDetails().toString(),
        "[Overall Score = 0]");

  }

}
