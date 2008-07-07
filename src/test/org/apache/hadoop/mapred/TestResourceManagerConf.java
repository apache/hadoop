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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.PrintWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

public class TestResourceManagerConf extends TestCase {

  private static final String TEST_CONF_FILE = 
                new File(".", "test-conf.xml").getAbsolutePath();
  private PrintWriter writer;
  private Map<String, String> defaultProperties;
  private ResourceManagerConf testConf;
  
  public TestResourceManagerConf() {
    defaultProperties = setupQueueProperties(
        new String[] { "guaranteed-capacity-maps", 
                       "guaranteed-capacity-reduces",
                       "reclaim-time-limit",
                       "allowed-users",
                       "denied-users",
                       "allowed-users-override",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { String.valueOf(Integer.MAX_VALUE), 
                        String.valueOf(Integer.MAX_VALUE),
                        "0", null, null, "false",
                        "true", "100" }
                      );
  }
  
  public void setUp() throws IOException {
    openFile();
  }
  
  public void tearDown() throws IOException {
    File testConfFile = new File(TEST_CONF_FILE);
    if (testConfFile.exists()) {
      testConfFile.delete();  
    }
  }
  
  public void testDefaults() {
    testConf = new ResourceManagerConf();
    Map<String, Map<String, String>> queueDetails
                            = new HashMap<String, Map<String,String>>();
    queueDetails.put("default", defaultProperties);
    checkQueueProperties(testConf, queueDetails);
  }
  
  public void testQueues() {

    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "guaranteed-capacity-maps", 
                       "guaranteed-capacity-reduces",
                       "reclaim-time-limit",
                       "allowed-users",
                       "denied-users",
                       "allowed-users-override",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "10", "5", "600", "u1,u2,u3", "u1,g1", "false",
                        "true", "25" }
                      );

    Map<String, String> q2Props = setupQueueProperties(
        new String[] { "guaranteed-capacity-maps", 
                       "guaranteed-capacity-reduces",
                       "reclaim-time-limit",
                       "allowed-users",
                       "denied-users",
                       "allowed-users-override",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "100", "50", "6000", "u4,u5,u6", "u6,g2", "true",
                        "false", "50" }
                      );

    startConfig();
    writeQueueDetails("default", q1Props);
    writeQueueDetails("research", q2Props);
    writeQueues("default,research");
    endConfig();

    testConf = new ResourceManagerConf(new Path(TEST_CONF_FILE));

    Map<String, Map<String, String>> queueDetails
              = new HashMap<String, Map<String,String>>();
    queueDetails.put("default", q1Props);
    queueDetails.put("research", q2Props);
    checkQueueProperties(testConf, queueDetails);
  }
  
  public void testQueueWithDefaultProperties() {
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "guaranteed-capacity-maps", 
                       "guaranteed-capacity-reduces",
                       "allowed-users",
                       "allowed-users-override",
                       "minimum-user-limit-percent" }, 
        new String[] { "20", "10", "u7,u8,u9", "false", "75" }
                      );
    startConfig();
    writeQueueDetails("default", q1Props);
    writeQueues("default");
    endConfig();

    testConf = new ResourceManagerConf(new Path(TEST_CONF_FILE));

    Map<String, Map<String, String>> queueDetails
              = new HashMap<String, Map<String,String>>();
    Map<String, String> expProperties = new HashMap<String, String>();
    for (String key : q1Props.keySet()) {
      expProperties.put(key, q1Props.get(key));
    }
    expProperties.put("reclaim-time-limit", "0");
    expProperties.put("denied-users", null);
    expProperties.put("supports-priority", "false");
    queueDetails.put("default", expProperties);
    checkQueueProperties(testConf, queueDetails);
  }

  public void testInvalidQueue() {
    try {
      testConf = new ResourceManagerConf();
      testConf.getGuaranteedCapacityMaps("invalid");
      fail("Should not return value for invalid queue");
    } catch (IllegalArgumentException iae) {
      assertEquals("Queue invalid is undefined.", iae.getMessage());
    }
  }

  public void testQueueAddition() {
    testConf = new ResourceManagerConf();
    Set<String> queues = new HashSet<String>();
    queues.add("default");
    queues.add("newqueue");
    testConf.setQueues(queues);
    
    testConf.setGuaranteedCapacityMaps("newqueue", 1);
    testConf.setGuaranteedCapacityReduces("newqueue", 1);
    testConf.setReclaimTimeLimit("newqueue", 30);
    testConf.setMinimumUserLimitPercent("newqueue", 40);
    testConf.setAllowedUsers("newqueue", "a", "b", "c");
    testConf.setDeniedUsers("newqueue", "d", "e", "f");
    testConf.setAllowedUsersOverride("newqueue", true);
    testConf.setPrioritySupported("newqueue", false);
    
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "guaranteed-capacity-maps", 
                       "guaranteed-capacity-reduces",
                       "reclaim-time-limit",
                       "allowed-users",
                       "denied-users",
                       "allowed-users-override",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "1", "1", "30", "a,b,c", "d,e,f", "true",
                        "false", "40" }
                      );
    
    Map<String, Map<String, String>> queueDetails = 
                  new HashMap<String, Map<String, String>>();
    queueDetails.put("default", defaultProperties);
    queueDetails.put("newqueue", q1Props);
    checkQueueProperties(testConf, queueDetails);
  }
  
  public void testReload() throws IOException {
    // use the setup in the test case testQueues as a base...
    testQueues();
    
    // write new values to the file...
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "guaranteed-capacity-maps", 
                       "guaranteed-capacity-reduces",
                       "reclaim-time-limit",
                       "allowed-users",
                       "denied-users",
                       "allowed-users-override",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "20", "5", "600", "u1,u2,u3,u4", "u1,g1", "false",
                        "true", "40" }
                      );

    Map<String, String> q2Props = setupQueueProperties(
        new String[] { "guaranteed-capacity-maps", 
                       "guaranteed-capacity-reduces",
                       "reclaim-time-limit",
                       "allowed-users",
                       "denied-users",
                       "allowed-users-override",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "1000", "500", "3000", "u4,u6", "g2", "false",
                        "false", "50" }
                      );

    openFile();
    startConfig();
    writeQueueDetails("default", q1Props);
    writeQueueDetails("production", q2Props);
    writeQueues("default,production");
    endConfig();
    
    testConf.reloadConfiguration();
    
    Map<String, Map<String, String>> queueDetails 
                      = new HashMap<String, Map<String, String>>();
    queueDetails.put("default", q1Props);
    queueDetails.put("production", q2Props);
    checkQueueProperties(testConf, queueDetails);
  }

  public void testPrint() throws IOException, InterruptedException {
    
    Map<String, String> q1Props = setupQueueProperties(
        new String[] { "guaranteed-capacity-maps", 
                       "guaranteed-capacity-reduces",
                       "reclaim-time-limit",
                       "allowed-users",
                       "denied-users",
                       "allowed-users-override",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "20", "5", "600", "u2,a1,c3,b4", "u1,g1", "false",
                        "true", "40" }
                      );

    Map<String, String> q2Props = setupQueueProperties(
        new String[] { "guaranteed-capacity-maps", 
                       "guaranteed-capacity-reduces",
                       "reclaim-time-limit",
                       "allowed-users",
                       "denied-users",
                       "allowed-users-override",
                       "supports-priority",
                       "minimum-user-limit-percent" }, 
        new String[] { "1000", "500", "3000", "e5,d6", "g2", "false",
                        "false", "50" }
                      );
    
    startConfig();
    writeQueueDetails("default", q1Props);
    writeQueueDetails("research", q2Props);
    writeQueues("default,research");
    endConfig();

    testConf = new ResourceManagerConf(new Path(TEST_CONF_FILE));
    
    PipedWriter pw = new PipedWriter();
    PrintWriter writer = new PrintWriter(pw);
    PipedReader pr = new PipedReader(pw);
    BufferedReader br = new BufferedReader(pr);
    
    PrintReaderThread prThread = new PrintReaderThread(br);
    prThread.start();

    testConf.printConfiguration(writer);
    writer.close();
    
    prThread.join();
    ArrayList<String> output = prThread.getOutput();
    
    ListIterator iter = output.listIterator();
    
    assertEquals("hadoop.rm.queue.names: default,research", iter.next());
    checkPrintOutput("default", q1Props, iter,
                      new String[] {"a1", "b4", "c3", "u2"},
                      new String[] {"g1", "u1"});
    checkPrintOutput("research", q2Props, iter,
                      new String[] {"d6", "e5" },
                      new String[] {"g2"});
  }
  
  private void checkPrintOutput(String queue, 
                                Map<String, String> queueProps,
                                ListIterator iter,
                                String[] allowedUsers,
                                String[] deniedUsers) {
    
    String[] propsOrder = {"guaranteed-capacity-maps", 
                             "guaranteed-capacity-reduces",
                             "reclaim-time-limit",
                             "minimum-user-limit-percent",
                             "supports-priority",
                             "allowed-users-override"};
    
    for (String prop : propsOrder) {
      assertEquals(("hadoop.rm.queue." + queue + "." + prop + ": " 
                    + queueProps.get(prop)), iter.next());
    }
    
    checkUserList(queue, iter, allowedUsers, "allowed-users");
    checkUserList(queue, iter, deniedUsers, "denied-users");
  }

  private void checkUserList(String queue, 
                              ListIterator iter, 
                              String[] userList,
                              String listType) {
    assertEquals("hadoop.rm.queue." + queue + "." + listType + ": ",
                    iter.next());
    for (String user : userList) {
      assertEquals("\t" + user, iter.next());
    }
  }

  private void checkQueueProperties(
                        ResourceManagerConf testConf,
                        Map<String, Map<String, String>> queueDetails) {
    Set<String> queues = testConf.getQueues();
    assertEquals(queueDetails.keySet().size(), queues.size());
    for (String name : queueDetails.keySet()) {
      assertTrue(queues.contains(name));  
    }
    
    for (String queueName : queueDetails.keySet()) {
      Map<String, String> map = queueDetails.get(queueName);
      assertEquals(Integer.parseInt(map.get("guaranteed-capacity-maps")),
          testConf.getGuaranteedCapacityMaps(queueName));
      assertEquals(Integer.parseInt(map.get("guaranteed-capacity-reduces")),
          testConf.getGuaranteedCapacityReduces(queueName));
      assertEquals(Integer.parseInt(map.get("minimum-user-limit-percent")),
          testConf.getMinimumUserLimitPercent(queueName));
      assertEquals(Integer.parseInt(map.get("reclaim-time-limit")),
          testConf.getReclaimTimeLimit(queueName));
      assertEquals(Boolean.parseBoolean(map.get("supports-priority")),
          testConf.isPrioritySupported(queueName));
      String[] expAllowedUsers = StringUtils.getStrings(
                                    map.get("allowed-users"));
      String[] allowedUsers = testConf.getAllowedUsers(queueName);
      assertTrue(Arrays.equals(expAllowedUsers, allowedUsers));
      String[] expDeniedUsers = StringUtils.getStrings(
                                    map.get("denied-users"));
      String[] deniedUsers = testConf.getDeniedUsers(queueName);
      assertTrue(Arrays.equals(expDeniedUsers, deniedUsers));
      assertEquals(Boolean.parseBoolean(map.get("allowed-users-override")),
          testConf.doAllowedUsersOverride(queueName));
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
    FileWriter fw = new FileWriter(TEST_CONF_FILE);
    BufferedWriter bw = new BufferedWriter(fw);
    writer = new PrintWriter(bw);
  }
  
  private void startConfig() {
    writer.println("<?xml version=\"1.0\"?>");
    writer.println("<configuration>");
  }
  
  private void writeQueues(String queueList) {
    writer.println("<property>");
    writer.println("<name>hadoop.rm.queue.names</name>");
    writer.println("<value>"+queueList+"</value>");
    writer.println("</property>");
  }
  
  private void writeQueueDetails(String queue, Map<String, String> props) {
    for (String key : props.keySet()) {
      writer.println("<property>");
      writer.println("<name>hadoop.rm.queue." + queue + "." + key +
                    "</name>");
      writer.println("<value>"+props.get(key)+"</value>");
      writer.println("</property>");
    }
  }
  
  private void endConfig() {
    writer.println("</configuration>");
    writer.close();
  }
  
  class PrintReaderThread extends Thread {
    
    private BufferedReader reader;
    private ArrayList<String> lines;
    
    public PrintReaderThread(BufferedReader reader) {
      this.reader = reader;
      lines = new ArrayList<String>();
    }
    
    public void run() {
      try {
        String line = reader.readLine();
        while (line != null) {
          lines.add(line);
          line = reader.readLine();
        }
      } catch (IOException ioe) {
        lines.clear();
      }
    }
    
    public ArrayList<String> getOutput() {
      return lines;
    }
  }
}
