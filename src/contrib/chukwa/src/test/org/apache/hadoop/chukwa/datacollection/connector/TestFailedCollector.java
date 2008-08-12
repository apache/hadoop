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

package org.apache.hadoop.chukwa.datacollection.connector;

import java.io.File;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.TempFileUtil;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;

import junit.framework.TestCase;

public class TestFailedCollector extends TestCase {
  
  public void testFailedCollector()
  {
    try {
      ChukwaAgent agent = new ChukwaAgent();
      boolean failed=false;
      HttpConnector connector = new HttpConnector(agent, "http://localhost:1234/chukwa");
      connector.start();
      
      ChukwaConfiguration cc = new ChukwaConfiguration();
      int portno = cc.getInt("chukwaAgent.control.port", 9093);
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);
      
      File tmpOutput = TempFileUtil.makeBinary(2000);
      
      cli.addFile("unknown", tmpOutput.getAbsolutePath());
      System.out.println("have " + agent.adaptorCount() + " running adaptors");
      cli.removeFile("unknown", tmpOutput.getAbsolutePath());
    
      
      tmpOutput.delete();
      assertFalse(failed);
      System.out.println("done");
      agent.shutdown();
      connector.shutdown();
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

}
