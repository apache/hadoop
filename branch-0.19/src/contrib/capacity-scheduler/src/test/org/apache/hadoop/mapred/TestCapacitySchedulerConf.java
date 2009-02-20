/** Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.mapred;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;

public class TestCapacitySchedulerConf extends TestCase {

  private static String testDataDir = System.getProperty("test.build.data");
  private static String testConfFile;
  
  private Map<String, String> defaultProperties;
  private CapacitySchedulerConf testConf;
  private PrintWriter writer;
  
  static {
    if (testDataDir == null) {
      testDataDir = ".";
    } else {
      new File(testDataDir).mkdirs();
    }
    testConfFile = new File(testDataDir, "test-conf.xml").getAbsolutePath();
  }
  
  public TestCapacitySchedulerConf() {
    defaultProperties = setupQueueProperties(
        new String[] { "guaranteed-capacity", 
                       "reclaim-time-limit",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "100", 
                        "300",
                        "false", 
                        "100" }
                      );
  }

  
  public void setUp() throws IOException {
    openFile();
  }
  
  public void tearDown() throws IOException {
    File confFile = new File(testConfFile);
    if (confFile.exists()) {
      confFile.delete();  
    }
  }
  
  public void testDefaults() {
    testConf = new CapacitySchedulerConf();
    Map<String, Map<String, String>> queueDetails
                            = new HashMap<String, Map<String,String>>();
    queueDetails.put("default", defaultProperties);
    checkQueueProperties(testConf, queueDetails);
  }
  
  public void testQueues() {

    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "guaranteed-capacity", 
                       "reclaim-time-limit",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "10", 
                        "600",
                        "true",
                        "25" }
                      );

    Map<String, String> q2Props = setupQueueProperties(
        new String[] { "guaranteed-capacity", 
                       "reclaim-time-limit",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "100", 
                        "6000",
                        "false", 
                        "50" }
                      );

    startConfig();
    writeQueueDetails("default", q1Props);
    writeQueueDetails("research", q2Props);
    endConfig();

    testConf = new CapacitySchedulerConf(new Path(testConfFile));

    Map<String, Map<String, String>> queueDetails
              = new HashMap<String, Map<String,String>>();
    queueDetails.put("default", q1Props);
    queueDetails.put("research", q2Props);
    checkQueueProperties(testConf, queueDetails);
  }
  
  public void testQueueWithDefaultProperties() {
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "guaranteed-capacity", 
                       "minimum-user-limit-percent" }, 
        new String[] { "20", 
                        "75" }
                      );
    startConfig();
    writeQueueDetails("default", q1Props);
    endConfig();

    testConf = new CapacitySchedulerConf(new Path(testConfFile));

    Map<String, Map<String, String>> queueDetails
              = new HashMap<String, Map<String,String>>();
    Map<String, String> expProperties = new HashMap<String, String>();
    for (String key : q1Props.keySet()) {
      expProperties.put(key, q1Props.get(key));
    }
    expProperties.put("reclaim-time-limit", "300");
    expProperties.put("supports-priority", "false");
    queueDetails.put("default", expProperties);
    checkQueueProperties(testConf, queueDetails);
  }

  public void testReload() throws IOException {
    // use the setup in the test case testQueues as a base...
    testQueues();
    
    // write new values to the file...
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "guaranteed-capacity", 
                       "reclaim-time-limit",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "20.5", 
                        "600",
                        "true", 
                        "40" }
                      );

    Map<String, String> q2Props = setupQueueProperties(
        new String[] { "guaranteed-capacity", 
                       "reclaim-time-limit",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "100", 
                        "3000",
                        "false",
                        "50" }
                      );

    openFile();
    startConfig();
    writeQueueDetails("default", q1Props);
    writeQueueDetails("production", q2Props);
    endConfig();
    
    testConf.reloadConfiguration();
    
    Map<String, Map<String, String>> queueDetails 
                      = new HashMap<String, Map<String, String>>();
    queueDetails.put("default", q1Props);
    queueDetails.put("production", q2Props);
    checkQueueProperties(testConf, queueDetails);
  }

  private void checkQueueProperties(
                        CapacitySchedulerConf testConf,
                        Map<String, Map<String, String>> queueDetails) {
    for (String queueName : queueDetails.keySet()) {
      Map<String, String> map = queueDetails.get(queueName);
      assertEquals(Float.parseFloat(map.get("guaranteed-capacity")),
           testConf.getGuaranteedCapacity(queueName));
      assertEquals(Integer.parseInt(map.get("minimum-user-limit-percent")),
          testConf.getMinimumUserLimitPercent(queueName));
      assertEquals(Integer.parseInt(map.get("reclaim-time-limit")),
          testConf.getReclaimTimeLimit(queueName));
      assertEquals(Boolean.parseBoolean(map.get("supports-priority")),
          testConf.isPrioritySupported(queueName));
    }
  }
  
  private Map<String, String> setupQueueProperties(String[] keys, 
                                                String[] values) {
    HashMap<String, String> map = new HashMap<String, String>();
    for(int i=0; i<keys.length; i++) {
      map.put(keys[i], values[i]);
    }
    return map;
  }

  private void openFile() throws IOException {
    
    if (testDataDir != null) {
      File f = new File(testDataDir);
      f.mkdirs();
    }
    FileWriter fw = new FileWriter(testConfFile);
    BufferedWriter bw = new BufferedWriter(fw);
    writer = new PrintWriter(bw);
  }
  
  private void startConfig() {
    writer.println("<?xml version=\"1.0\"?>");
    writer.println("<configuration>");
  }
  
  private void writeQueueDetails(String queue, Map<String, String> props) {
    for (String key : props.keySet()) {
      writer.println("<property>");
      writer.println("<name>mapred.capacity-scheduler.queue." 
                        + queue + "." + key +
                    "</name>");
      writer.println("<value>"+props.get(key)+"</value>");
      writer.println("</property>");
    }
  }
  
  private void endConfig() {
    writer.println("</configuration>");
    writer.close();
  }
  
}
