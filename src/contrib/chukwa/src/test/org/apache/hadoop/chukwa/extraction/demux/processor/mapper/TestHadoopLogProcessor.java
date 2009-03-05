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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*
 * Test code for verifying that the log processors work properly.
 * 
 * Currently more or less just a stub
 */
public class TestHadoopLogProcessor extends TestCase{
  
  long serializedSize = 0;
  OutputCollector<ChukwaRecordKey, ChukwaRecord> nullcollector = new OutputCollector<ChukwaRecordKey, ChukwaRecord>() {
    public void collect(ChukwaRecordKey arg0, ChukwaRecord arg1) throws IOException
    {
      serializedSize += arg1.toString().length();
    }
  };

  
  
  public void testHLPParseTimes() {
    HadoopLogProcessor hlp = new HadoopLogProcessor();
  
    int LINES = 50000;
    long bytes = 0;
    long ts_start = System.currentTimeMillis();
    for(int i =0; i < LINES; ++i) {
      Chunk c = getNewChunk();
      bytes += c.getData().length;
      hlp.process(null,c, nullcollector, Reporter.NULL);
 //     hlp.parse(line, nullcollector, Reporter.NULL);
    }
    long time = (System.currentTimeMillis() - ts_start);
    System.out.println("parse took " + time + " milliseconds");
    System.out.println("aka " + time * 1.0 / LINES + " ms per line or " + 
        time *1000.0 / bytes  + " ms per kilobyte of log data");
    System.out.println("output records had total length of " + serializedSize);
  }
  

  java.util.Random r = new java.util.Random();
  public Chunk getNewChunk() {
    int ms = r.nextInt(1000);
    String line = "2008-05-29 10:42:22,"+ ms + " INFO org.apache.hadoop.dfs.DataNode: Some text goes here" +r.nextInt() + "\n";
    ChunkImpl c = new ChunkImpl("HadoopLogProcessor", "test" ,line.length() -1 ,line.getBytes() , null );
       
    return c;
  }
  
}

