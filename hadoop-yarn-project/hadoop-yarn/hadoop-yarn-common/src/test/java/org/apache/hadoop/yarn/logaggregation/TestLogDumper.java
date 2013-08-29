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

package org.apache.hadoop.yarn.logaggregation;

import static org.junit.Assert.assertTrue;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

public class TestLogDumper {
  ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;

  @Before
  public void setUp() {
    sysOutStream = new ByteArrayOutputStream();
    sysOut =  new PrintStream(sysOutStream);
    System.setOut(sysOut);
  }

  @Test
  public void testFailResultCodes() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setClass("fs.file.impl", LocalFileSystem.class, FileSystem.class);
    LogDumper dumper = new LogDumper();
    dumper.setConf(conf);
    
    // verify dumping a non-existent application's logs returns a failure code
    int exitCode = dumper.run( new String[] {
        "-applicationId", "application_0_0" } );
    assertTrue("Should return an error code", exitCode != 0);
    
    // verify dumping a non-existent container log is a failure code 
    exitCode = dumper.dumpAContainersLogs("application_0_0", "container_0_0",
        "nonexistentnode:1234", "nobody");
    assertTrue("Should return an error code", exitCode != 0);
  }

  @Test
  public void testHelpMessage() throws Exception {
    Configuration conf = new YarnConfiguration();
    LogDumper dumper = new LogDumper();
    dumper.setConf(conf);

    int exitCode = dumper.run(new String[]{});
    assertTrue(exitCode == -1);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Retrieve logs for completed YARN applications.");
    pw.println("usage: yarn logs -applicationId <application ID> [OPTIONS]");
    pw.println();
    pw.println("general options are:");
    pw.println(" -appOwner <Application Owner>   AppOwner (assumed to be current user if");
    pw.println("                                 not specified)");
    pw.println(" -containerId <Container ID>     ContainerId (must be specified if node");
    pw.println("                                 address is specified)");
    pw.println(" -nodeAddress <Node Address>     NodeAddress in the format nodename:port");
    pw.println("                                 (must be specified if container id is");
    pw.println("                                 specified)");
    pw.close();
    String appReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appReportStr, sysOutStream.toString());
  }
}
