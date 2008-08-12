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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;

import junit.framework.TestCase;

public class TestStartAtOffset extends TestCase {
  
  ChunkCatcherConnector chunks;
  public TestStartAtOffset() {
    chunks = new ChunkCatcherConnector();
    chunks.start();
  }
  
  public void testStartAtOffset() throws IOException, InterruptedException, ChukwaAgent.AlreadyRunningException {
    ChukwaAgent  agent = new ChukwaAgent();
    File testFile = makeTestFile();
    int startOffset = 50;
    agent.processCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8 " +
         "lines "+ startOffset+ " " + testFile + " " + startOffset);
    assertTrue(agent.adaptorCount() == 1);
    System.out.println("getting a chunk...");
    Chunk c = chunks.waitForAChunk(); 
    System.out.println("got chunk");
    assertTrue(c.getSeqID() == testFile.length() + startOffset);    
    
    assertTrue(c.getRecordOffsets().length == 80);//80 lines in test file
    int recStart = 0;
    for(int rec = 0 ; rec < c.getRecordOffsets().length; ++rec) {
      String record = new String(c.getData(), recStart, c.getRecordOffsets()[rec] - recStart+1);
      System.out.println("record "+ rec+ " was: " + record);
      assertTrue(record.equals(rec + " abcdefghijklmnopqrstuvwxyz\n"));
      recStart = c.getRecordOffsets()[rec] +1;
    }
    assertTrue(c.getDataType().equals("lines"));    
    agent.shutdown();
  }
  
  public void testStartAfterOffset() throws IOException, InterruptedException, ChukwaAgent.AlreadyRunningException {
    ChukwaAgent  agent = new ChukwaAgent();
    File testFile = makeTestFile();
    int startOffset = 50;
    agent.processCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8 " +
         "lines "+ startOffset+ " " + testFile + " " + (startOffset + 29) );
    assertTrue(agent.adaptorCount() == 1);
    System.out.println("getting a chunk...");
    Chunk c = chunks.waitForAChunk(); 
    System.out.println("got chunk");
    assertTrue(c.getSeqID() == testFile.length() + startOffset);    
    
    assertTrue(c.getRecordOffsets().length == 79);//80 lines in test file, minus the one we skipped
    int recStart = 0;
    for(int rec = 0 ; rec < c.getRecordOffsets().length; ++rec) {
      String record = new String(c.getData(), recStart, c.getRecordOffsets()[rec] - recStart+1);
      System.out.println("record "+ rec+ " was: " + record);
      assertTrue(record.equals((rec+1) + " abcdefghijklmnopqrstuvwxyz\n"));
      recStart = c.getRecordOffsets()[rec] +1;
    }
    assertTrue(c.getDataType().equals("lines"));    
    agent.shutdown();
  }
  
  private File makeTestFile() throws IOException {
    File tmpOutput = new File("/tmp/chukwaTest");
    FileOutputStream fos = new FileOutputStream(tmpOutput);
    
    PrintWriter pw = new PrintWriter(fos);
    for(int i = 0; i < 80; ++i) {
      pw.print(i + " ");
      pw.println("abcdefghijklmnopqrstuvwxyz");
    }
    pw.flush();
    pw.close();
    return tmpOutput;
  }
  
  
}
