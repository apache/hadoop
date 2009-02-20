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

package org.apache.hadoop.chukwa.datacollection;

import java.io.*;
import java.util.*;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.*;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.datacollection.writer.*;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import junit.framework.TestCase;

/**
 * Starts an agent, connector, collector in one process.
 * Starts an adaptor to push a random temp file through through.
 * 
 *
 */
public class TestAgentCollector extends TestCase {
  static Server server;
  static Context root;
  static ChukwaAgent agent ;
  static HttpConnector connector;
  int STOPS_AND_STARTS = 10;
  
  static {
    try {
    server = new Server(9990);
    root = new Context(server,"/",Context.SESSIONS);
    agent = new ChukwaAgent();
    connector = new HttpConnector(agent, "http://localhost:9990/chukwa");
    connector.start();

    ConsoleWriter readInData =  new ConsoleWriter(false);
    ServletCollector.setWriter(readInData);
    root.addServlet(new ServletHolder(new ServletCollector()), "/*");
    server.start();
    server.setStopAtShutdown(false);
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
  /**
   * @param args
   */
  public void testAllOnce()
  {
    try {

      InMemoryWriter readInData =  new InMemoryWriter();
      ServletCollector.setWriter(readInData);
  
      Thread.sleep(1000);
        
      ChukwaConfiguration cc = new ChukwaConfiguration();
      int portno = cc.getInt("chukwaAgent.control.port", 9093);
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);
      
      File tmpOutput = new File("/tmp/chukwaTest");
      FileOutputStream fos = new FileOutputStream(tmpOutput);
      Random r = new Random();
      boolean failed = false;
      byte[] randomData = new byte[2000];
      r.nextBytes(randomData);
      randomData[1999] = '\n';//need data to end with \n since default tailer uses that
      fos.write(randomData);
      fos.flush();
      fos.close();
      
      cli.addFile("unknown", tmpOutput.getAbsolutePath());
      assertEquals(1, agent.adaptorCount());
      cli.removeFile("unknown", tmpOutput.getAbsolutePath());
      assertEquals(0, agent.adaptorCount());
      org.apache.hadoop.chukwa.Chunk readIn = readInData.readOutChunk(randomData.length, 5000);
      byte[] readInBytes = readIn.getData();
      if(readInBytes.length != randomData.length)
      {
        System.err.println("FAIL: input ended at " + readInBytes.length + " bytes");
        failed = true;
      } else {
        for(int i = 0; i < randomData.length ; ++i) {
          byte next = readInBytes[i];
          if(next != randomData[i]) {
            System.err.println("FAIL: saw byte " + next + " at position " + i +
                ", expected " + randomData[i]);
            failed = true;
            break;
          }
        }
      }
      cli.removeAll();
      tmpOutput.delete();
      assertFalse(failed);
      System.out.println("done");
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
  
  public void testStopAndStart() {

    try {
      ConsoleWriter readInData =  new ConsoleWriter(false);
      ServletCollector.setWriter(readInData);
  
      Thread.sleep(1000);
       
      ChukwaConfiguration cc = new ChukwaConfiguration();
      int portno = cc.getInt("chukwaAgent.control.port", 9093);
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);
      
      for(int i=1; i < STOPS_AND_STARTS; ++i) {
        cli.add("org.apache.hadoop.chukwa.util.ConstRateAdaptor", "oneatatime_raw" + i, "20000", 0);
        assertEquals("adaptor failed to start", 1, agent.adaptorCount());
        Thread.sleep(2000);   
        cli.removeAll();
        assertTrue("adaptor failed to stop", agent.adaptorCount() == 0);
      }
    } catch(Exception e) {
      e.printStackTrace();
    }
    
  }

  public void testManyAdaptors() {
    try {
      
      ConsoleWriter readInData =  new ConsoleWriter(false);
      ServletCollector.setWriter(readInData);
  
      Thread.sleep(1000);

      
      ChukwaConfiguration cc = new ChukwaConfiguration();
      int portno = cc.getInt("chukwaAgent.control.port", 9093);
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);
      assertEquals("no adaptors should be running pre-test",0, agent.adaptorCount());
      for(int i=0; i < 10; ++i) {
        
        for(int n = 1; n < 7; ++n) {
//          cli.add("org.apache.hadoop.chukwa.util.ConstRateAdaptor", "many_raw"+n, "20000", 0);
          agent.processCommand("add org.apache.hadoop.chukwa.util.ConstRateAdaptor many_raw"+n + " 20000 0");
          assertEquals(n, agent.adaptorCount());
        }
        
        Thread.sleep(5000);
   
        cli.removeAll();
        assertEquals("remove left some adaptors running", 0, agent.adaptorCount());
      }
  } catch(Exception e) {
    e.printStackTrace();
  }
  }
  
  public static void main(String[] args)  {
    new TestAgentCollector().testManyAdaptors();
  }
  

}
