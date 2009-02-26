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

package org.apache.hadoop.chukwa.inputtools;

import java.io.IOException;

import org.apache.hadoop.mapred.Reporter;

import junit.framework.TestCase;

import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class TestInputFormat extends TestCase {
  
  String[] lines = {
    "the rain",
    "in spain",
    "falls mainly",
    "in the plain"
  };
  
  public void testInputFormat() {
    
    try {
    JobConf conf = new JobConf();
    String TMP_DIR = System.getProperty("test.build.data", "/tmp");
    Path filename = new Path("file:///"+TMP_DIR+"/tmpSeqFile");
    SequenceFile.Writer sfw = SequenceFile.createWriter(FileSystem.getLocal(conf),
        conf, filename, ChukwaArchiveKey.class, ChunkImpl.class,
        SequenceFile.CompressionType.NONE, Reporter.NULL);
    
    
    StringBuilder buf = new StringBuilder();
    int offsets[] = new int[lines.length];
    for(int i= 0; i < lines.length; ++i) {
      buf.append(lines[i]);
      buf.append("\n");
      offsets[i] = buf.length()-1;
    }
    ChukwaArchiveKey key = new ChukwaArchiveKey(0, "datatype", "sname", 0);
    ChunkImpl val = new ChunkImpl("datatype", "sname", 0, buf.toString().getBytes(), null);
    val.setRecordOffsets(offsets);
    sfw.append(key, val);
    sfw.append(key, val); //write it twice
    sfw.close();
    
    
    long len = FileSystem.getLocal(conf).getFileStatus(filename).getLen();
    InputSplit split = new FileSplit(filename, 0, len, (String[] ) null);
    ChukwaInputFormat in = new ChukwaInputFormat();
    RecordReader<LongWritable, Text> r= in.getRecordReader(split, conf, Reporter.NULL);
    

    LongWritable l = r.createKey();
    Text line = r.createValue();
    for(int i =0 ; i < lines.length * 2; ++i) {
      boolean succeeded = r.next(l, line);
      assertTrue(succeeded);
      assertEquals(i, l.get());
      assertEquals(lines[i % lines.length] , line.toString());
      System.out.println("read line: "+ l.get() + " "+ line);
    }
    boolean succeeded = r.next(l, line);
    assertFalse(succeeded);
    
    } catch(IOException e) {
      e.printStackTrace();
      fail("IO exception "+ e);
    }
  }

}
