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
package org.apache.hadoop.chukwa.extraction.demux;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import junit.framework.TestCase;
/**
 * test the Demux job in one process, using mini-mr.
 * 
 * Unfortunately, this test case needs more jars than the rest of chukwa,
 *  including hadoop-*-test, commons-cli, and jetty5
 *  
 *  
 * 
 */
public class TestDemux extends TestCase {

  java.util.Random r = new java.util.Random();
  public ChunkImpl getARandomChunk() {
    int ms = r.nextInt(1000);
    String line = "2008-05-29 10:42:22,"+ ms + " INFO org.apache.hadoop.dfs.DataNode: Some text goes here" +r.nextInt() + "\n";

    ChunkImpl c = new ChunkImpl("HadoopLogProcessor", "test", line.length() -1L, line.getBytes(), null);
    return c;
  }
  

  public void writeASinkFile(Configuration conf, FileSystem fileSys, Path dest, int chunks) throws IOException {
    FSDataOutputStream out = fileSys.create(dest);

    Calendar calendar = Calendar.getInstance();
    SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(conf, out,
        ChukwaArchiveKey.class, ChunkImpl.class,
        SequenceFile.CompressionType.NONE, null);
    for(int i=0; i < chunks; ++i) {
      ChunkImpl chunk = getARandomChunk();
      ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();
      // FIXME compute this once an hour
      calendar.setTimeInMillis(System.currentTimeMillis());
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      archiveKey.setTimePartition(calendar.getTimeInMillis());
      archiveKey.setDataType(chunk.getDataType());
      archiveKey.setStreamName(chunk.getStreamName());
      archiveKey.setSeqId(chunk.getSeqID());
      seqFileWriter.append(archiveKey, chunk);
    }
    seqFileWriter.close();
    out.close();
  }
  
  private void runDemux(JobConf job, Path sortInput, Path sortOutput) 
  throws Exception {
    // Setup command-line arguments to 'sort'
    String[] sortArgs = {sortInput.toString(), sortOutput.toString()};
    
    // Run Sort
    assertEquals(ToolRunner.run(job, new Demux(), sortArgs), 0);
  }
  
  int NUM_HADOOP_SLAVES = 1;
  int LINES = 10000;
  private static final Path DEMUX_INPUT_PATH = new Path("/demux/input");
  private static final Path DEMUX_OUTPUT_PATH = new Path("/demux/output");

  public void testDemux() {
    try{
      System.out.println("testing demux");
      Configuration conf = new Configuration();
      System.setProperty("hadoop.log.dir", "/tmp/");
      MiniDFSCluster dfs = new MiniDFSCluster(conf, NUM_HADOOP_SLAVES, true, null);
      FileSystem fileSys = dfs.getFileSystem();
      MiniMRCluster mr = new MiniMRCluster(NUM_HADOOP_SLAVES, fileSys.getUri().toString(), 1);
      writeASinkFile(conf, fileSys, DEMUX_INPUT_PATH, LINES);

      System.out.println("wrote " + 
      fileSys.getFileStatus(DEMUX_INPUT_PATH).getLen() + " bytes of temp test data");
      long ts_start = System.currentTimeMillis();
      runDemux(mr.createJobConf(), DEMUX_INPUT_PATH, DEMUX_OUTPUT_PATH);

      long time = (System.currentTimeMillis() - ts_start);
      long bytes = fileSys.getContentSummary(DEMUX_OUTPUT_PATH).getLength();
      System.out.println("result was " + bytes + " bytes long");
      System.out.println("processing took " + time + " milliseconds");
      System.out.println("aka " + time * 1.0 / LINES + " ms per line or " + 
          time *1000.0 / bytes  + " ms per kilobyte of log data");
      
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
  
  
}
