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

package org.apache.hadoop.yarn.client.cli;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Class for testing {@link SchedConfCLI}.
 */
public class TestSchedConfCLI {

  private ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;

  private ByteArrayOutputStream sysErrStream;
  private PrintStream sysErr;

  private SchedConfCLI cli;

  @Before
  public void setUp() {
    sysOutStream = new ByteArrayOutputStream();
    sysOut =  new PrintStream(sysOutStream);
    System.setOut(sysOut);

    sysErrStream = new ByteArrayOutputStream();
    sysErr = new PrintStream(sysErrStream);
    System.setErr(sysErr);

    cli = new SchedConfCLI();
  }

  @Test(timeout = 10000)
  public void testInvalidConf() throws Exception {
    // conf pair with no key should be invalid
    int exitCode = cli.run(new String[] {"-add", "root.a:=confVal"});
    assertTrue("Should return an error code", exitCode != 0);
    assertTrue(sysErrStream.toString().contains("Specify configuration key " +
        "value as confKey=confVal."));
    exitCode = cli.run(new String[] {"-update", "root.a:=confVal"});
    assertTrue("Should return an error code", exitCode != 0);
    assertTrue(sysErrStream.toString().contains("Specify configuration key " +
        "value as confKey=confVal."));

    exitCode = cli.run(new String[] {"-add", "root.a:confKey=confVal=conf"});
    assertTrue("Should return an error code", exitCode != 0);
    assertTrue(sysErrStream.toString().contains("Specify configuration key " +
        "value as confKey=confVal."));
    exitCode = cli.run(new String[] {"-update", "root.a:confKey=confVal=c"});
    assertTrue("Should return an error code", exitCode != 0);
    assertTrue(sysErrStream.toString().contains("Specify configuration key " +
        "value as confKey=confVal."));
  }

  @Test(timeout = 10000)
  public void testAddQueues() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.addQueues("root.a:a1=aVal1,a2=aVal2,a3=", schedUpdateInfo);
    QueueConfigInfo addInfo = schedUpdateInfo.getAddQueueInfo().get(0);
    assertEquals("root.a", addInfo.getQueue());
    Map<String, String> params = addInfo.getParams();
    assertEquals(3, params.size());
    assertEquals("aVal1", params.get("a1"));
    assertEquals("aVal2", params.get("a2"));
    assertNull(params.get("a3"));

    schedUpdateInfo = new SchedConfUpdateInfo();
    cli.addQueues("root.b:b1=bVal1;root.c:c1=cVal1", schedUpdateInfo);
    assertEquals(2, schedUpdateInfo.getAddQueueInfo().size());
    QueueConfigInfo bAddInfo = schedUpdateInfo.getAddQueueInfo().get(0);
    assertEquals("root.b", bAddInfo.getQueue());
    Map<String, String> bParams = bAddInfo.getParams();
    assertEquals(1, bParams.size());
    assertEquals("bVal1", bParams.get("b1"));
    QueueConfigInfo cAddInfo = schedUpdateInfo.getAddQueueInfo().get(1);
    assertEquals("root.c", cAddInfo.getQueue());
    Map<String, String> cParams = cAddInfo.getParams();
    assertEquals(1, cParams.size());
    assertEquals("cVal1", cParams.get("c1"));
  }

  @Test(timeout = 10000)
  public void testRemoveQueues() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.removeQueues("root.a;root.b;root.c.c1", schedUpdateInfo);
    List<String> removeInfo = schedUpdateInfo.getRemoveQueueInfo();
    assertEquals(3, removeInfo.size());
    assertEquals("root.a", removeInfo.get(0));
    assertEquals("root.b", removeInfo.get(1));
    assertEquals("root.c.c1", removeInfo.get(2));
  }

  @Test(timeout = 10000)
  public void testUpdateQueues() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.updateQueues("root.a:a1=aVal1,a2=aVal2,a3=", schedUpdateInfo);
    QueueConfigInfo updateInfo = schedUpdateInfo.getUpdateQueueInfo().get(0);
    assertEquals("root.a", updateInfo.getQueue());
    Map<String, String> params = updateInfo.getParams();
    assertEquals(3, params.size());
    assertEquals("aVal1", params.get("a1"));
    assertEquals("aVal2", params.get("a2"));
    assertNull(params.get("a3"));

    schedUpdateInfo = new SchedConfUpdateInfo();
    cli.updateQueues("root.b:b1=bVal1;root.c:c1=cVal1", schedUpdateInfo);
    assertEquals(2, schedUpdateInfo.getUpdateQueueInfo().size());
    QueueConfigInfo bUpdateInfo = schedUpdateInfo.getUpdateQueueInfo().get(0);
    assertEquals("root.b", bUpdateInfo.getQueue());
    Map<String, String> bParams = bUpdateInfo.getParams();
    assertEquals(1, bParams.size());
    assertEquals("bVal1", bParams.get("b1"));
    QueueConfigInfo cUpdateInfo = schedUpdateInfo.getUpdateQueueInfo().get(1);
    assertEquals("root.c", cUpdateInfo.getQueue());
    Map<String, String> cParams = cUpdateInfo.getParams();
    assertEquals(1, cParams.size());
    assertEquals("cVal1", cParams.get("c1"));
  }

  @Test(timeout = 10000)
  public void testGlobalUpdate() {
    SchedConfUpdateInfo schedUpdateInfo = new SchedConfUpdateInfo();
    cli.globalUpdates("schedKey1=schedVal1,schedKey2=schedVal2",
        schedUpdateInfo);
    Map<String, String> globalInfo = schedUpdateInfo.getGlobalParams();
    assertEquals(2, globalInfo.size());
    assertEquals("schedVal1", globalInfo.get("schedKey1"));
    assertEquals("schedVal2", globalInfo.get("schedKey2"));
  }
}
