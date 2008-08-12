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
package org.apache.hadoop.chukwa.datacollection.adaptor;

import junit.framework.TestCase;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;

public class TestExecAdaptor extends TestCase {
  
  ChunkCatcherConnector chunks;
  public TestExecAdaptor() {
    chunks = new ChunkCatcherConnector();
    chunks.start();
  }
  
  public void testWithPs() throws ChukwaAgent.AlreadyRunningException {
    try {
      ChukwaAgent  agent = new ChukwaAgent();
      agent.processCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor ps ps aux 0");
  
      Chunk c = chunks.waitForAChunk();
      System.out.println(new String(c.getData()));
    } catch(InterruptedException e) {
      
    }
  }

}
