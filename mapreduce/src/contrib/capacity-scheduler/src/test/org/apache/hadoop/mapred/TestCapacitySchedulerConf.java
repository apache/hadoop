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
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;

public class TestCapacitySchedulerConf extends TestCase {

  private static String testDataDir = System.getProperty("test.build.data");
  private static String testConfFile;
  
  //private Map<String, String> defaultProperties;
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


  public void testInitializationPollerProperties() 
    throws Exception {
    /*
     * Test case to check properties of poller when no configuration file
     * is present.
     */
    testConf = new CapacitySchedulerConf();
    long pollingInterval = testConf.getSleepInterval();
    int maxWorker = testConf.getMaxWorkerThreads();
    assertTrue("Invalid polling interval ",pollingInterval > 0);
    assertTrue("Invalid working thread pool size" , maxWorker > 0);
    
    //test case for custom values configured for initialization 
    //poller.
    openFile();
    startConfig();
    writeProperty("mapred.capacity-scheduler.init-worker-threads", "1");
    writeProperty("mapred.capacity-scheduler.init-poll-interval", "1");
    endConfig();
    
    testConf = new CapacitySchedulerConf(new Path(testConfFile));
    
    pollingInterval = testConf.getSleepInterval();
    
    maxWorker = testConf.getMaxWorkerThreads();
    
    assertEquals("Invalid polling interval ",pollingInterval ,1);
    assertEquals("Invalid working thread pool size" , maxWorker, 1);
    
    //Test case for invalid values configured for initialization
    //poller
    openFile();
    startConfig();
    writeProperty("mapred.capacity-scheduler.init-worker-threads", "0");
    writeProperty("mapred.capacity-scheduler.init-poll-interval", "0");
    endConfig();
    
    testConf = new CapacitySchedulerConf(new Path(testConfFile));
    
    try {
      pollingInterval = testConf.getSleepInterval();
      fail("Polling interval configured is illegal");
    } catch (IllegalArgumentException e) {}
    try {
      maxWorker = testConf.getMaxWorkerThreads();
      fail("Max worker thread configured is illegal");
    } catch (IllegalArgumentException e) {}
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


  private void writeProperty(String name, String value) {
    writer.println("<property>");
    writer.println("<name> " + name + "</name>");
    writer.println("<value>"+ value+"</value>");
    writer.println("</property>");
    
  }
  
  private void endConfig() {
    writer.println("</configuration>");
    writer.close();
  }

  public void testConfigurationValuesConversion() throws IOException {
    Properties prp = new Properties();

    prp.setProperty("capacity","10");
    prp.setProperty("maximum-capacity","20.5");
    prp.setProperty("supports-priority","false");
    prp.setProperty("minimum-user-limit-percent","23");

    CapacitySchedulerConf conf = new CapacitySchedulerConf();
    conf.setProperties("default",prp);

    assertTrue(conf.getCapacity("default") == 10f);
    assertTrue(conf.getMaxCapacity("default") == 20.5f);
    assertTrue(conf.isPrioritySupported("default") == false);
    assertTrue(conf.getMinimumUserLimitPercent("default")==23);

    //check for inproper stuff
    prp.setProperty("capacity","h");
    prp.setProperty("maximum-capacity","20");

    //This is because h is invalid value.
    assertTrue(conf.getCapacity("default") == -1);
    
    assertFalse(conf.getMaxCapacity("default") != 20);
  }
  
}
