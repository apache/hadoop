/*
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
package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import java.io.*;

import junit.framework.TestCase;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;

import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.adaptor.*;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;

public class TestRawAdaptor extends TestCase {
  ChunkCatcherConnector chunks;
  public TestRawAdaptor() {
    chunks = new ChunkCatcherConnector();
    chunks.start();
  }
  
  public void testRawAdaptor() throws IOException, InterruptedException, ChukwaAgent.AlreadyRunningException {

    ChukwaAgent  agent = new ChukwaAgent();
    // Remove any adaptor left over from previous run
    ChukwaConfiguration cc = new ChukwaConfiguration();
    int portno = cc.getInt("chukwaAgent.control.port", 9093);
    ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);
    cli.removeAll();
    // sleep for some time to make sure we don't get chunk from existing streams
    Thread.sleep(5000);
    File testFile = makeTestFile("chukwaRawTest",80);
    long adaptorId = agent.processCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.FileTailingAdaptor" +
        " raw " + testFile + " 0");
    assertTrue(adaptorId != -1);
    Chunk c = chunks.waitForAChunk();
    while(!c.getDataType().equals("raw")) {
        c = chunks.waitForAChunk();
    }
    assertTrue(c.getDataType().equals("raw"));
    assertTrue(c.getRecordOffsets().length == 1);
    assertTrue(c.getSeqID() == testFile.length());
    agent.stopAdaptor(adaptorId, false);
    agent.shutdown();
  }

  private File makeTestFile(String name, int size) throws IOException {
    File tmpOutput = new File(System.getProperty("test.build.data", "/tmp"),name);
    FileOutputStream fos = new FileOutputStream(tmpOutput);
    
    PrintWriter pw = new PrintWriter(fos);
    for(int i = 0; i < size; ++i) {
      pw.print(i + " ");
      pw.println("abcdefghijklmnopqrstuvwxyz");
    }
    pw.flush();
    pw.close();
    return tmpOutput;
  }
  
  public static void main(String[] args) {
    try {
      TestRawAdaptor tests = new TestRawAdaptor();
      tests.testRawAdaptor();
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
  
}
