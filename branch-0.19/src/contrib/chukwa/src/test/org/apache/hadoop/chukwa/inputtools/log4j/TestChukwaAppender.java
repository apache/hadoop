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

package org.apache.hadoop.chukwa.inputtools.log4j;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;

import junit.framework.TestCase;
import java.io.*;
import org.apache.log4j.*;
import org.apache.log4j.spi.*;

public class TestChukwaAppender extends TestCase {
  
  public void testChukwaAppender() {
    try {
    
    ChukwaAgent agent = new ChukwaAgent();
    ChunkCatcherConnector chunks = new ChunkCatcherConnector();
    chunks.start();
    Logger myLogger = Logger.getLogger(TestChukwaAppender.class);
    File out = new File("/tmp/chukwa_test_out");
    out.delete();
    ChukwaDailyRollingFileAppender app = new ChukwaDailyRollingFileAppender(
        new SimpleLayout(), out.getAbsolutePath(), "yyyy-MM-dd");
    app.append(new LoggingEvent("foo", myLogger,  System.currentTimeMillis(),Priority.INFO, "foo", null));
    assertEquals(1, agent.adaptorCount());
    Chunk c = chunks.waitForAChunk();
    System.out.println("read a chunk OK");
    String logLine = new String(c.getData());
    assertTrue(logLine.equals("INFO - foo\n"));
    System.out.println(new String(c.getData()));
    //
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

}
