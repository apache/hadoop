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
package org.apache.hadoop.chukwa.datacollection.agent;

import java.util.ArrayList;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.datacollection.test.ConsoleOutConnector;

import junit.framework.TestCase;

public class TestAgent extends TestCase {


  public void testStopAndStart() {

    try {
      ChukwaAgent agent = new ChukwaAgent();
      ConsoleOutConnector conn = new ConsoleOutConnector(agent, true);
      conn.start();
      
      ChukwaConfiguration cc = new ChukwaConfiguration();
      int portno = cc.getInt("chukwaAgent.control.port", 9093);
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);
      
      for(int i=1; i < 20; ++i) {
        cli.add("org.apache.hadoop.chukwa.util.ConstRateAdaptor", "raw" + i, "20000", 0);
        assertTrue(agent.adaptorCount() == 1);
        Thread.sleep(2000);   
        cli.removeAll();
        assertTrue(agent.adaptorCount() == 0);
      }
      agent.shutdown();
      conn.shutdown();
    } catch(Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }
  
  public void testMultiStopAndStart() {

    try {
      ChukwaAgent agent = new ChukwaAgent();
      ConsoleOutConnector conn = new ConsoleOutConnector(agent, true);
      conn.start();
      
      for(int trial=0; trial < 20; ++trial) {
        ArrayList<Long> runningAdaptors = new ArrayList<Long>();
       
        for(int i = 1; i < 7; ++i) {
          long l = agent.processCommand("add org.apache.hadoop.chukwa.util.ConstRateAdaptor raw"+i+ " 20000 0");
          assertTrue(agent.adaptorCount() == i); 
          assertTrue(l != -1);
          runningAdaptors.add(l);
        }
        Thread.sleep(1000);   
        for(Long l: runningAdaptors)
          agent.stopAdaptor(l, true);
        assertTrue(agent.adaptorCount() == 0);
      }
      agent.shutdown();
    } catch(Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }

}
