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

package org.apache.hadoop.chukwa.datacollection.collector;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.sender.*;
import org.apache.hadoop.chukwa.datacollection.writer.*;

import java.util.*;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class CollectorTest extends TestCase {
  
  
  public void testCollector() {
    try {
      Configuration conf = new Configuration();
      conf.set("chukwaCollector.chunkSuppressBufferSize", "10");
      conf.set("chukwaCollector.pipeline", 
          "org.apache.hadoop.chukwa.datacollection.writer.Dedup,"//note comma
           + "org.apache.hadoop.chukwa.datacollection.collector.CaptureWriter");
      conf.set("chukwaCollector.writerClass", PipelineStageWriter.class.getCanonicalName());
      ChukwaHttpSender sender = new ChukwaHttpSender(conf);
      ArrayList<String> collectorList = new ArrayList<String>();
      collectorList.add("http://localhost:9990/chukwa");
      sender.setCollectors(new RetryListOfCollectors(collectorList, 50));
      Server server = new Server(9990);
      Context root = new Context(server,"/",Context.SESSIONS);
  
      root.addServlet(new ServletHolder(new ServletCollector(conf)), "/*");
      server.start();
      server.setStopAtShutdown(false);
      Thread.sleep(1000);
      
      Chunk c = new ChunkImpl("data", "stream", 0, "testing -- this should appear once".getBytes(), null);
      ArrayList<Chunk> toSend = new ArrayList<Chunk>();
      toSend.add(c);
      toSend.add(c);
      sender.send(toSend);
      Thread.sleep(1000);
      assertEquals(1, CaptureWriter.outputs.size());
    } catch(Exception e) {
      fail(e.toString());
    }
    
  }

}
