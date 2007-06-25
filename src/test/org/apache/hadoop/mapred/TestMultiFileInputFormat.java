/**
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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

public class TestMultiFileInputFormat extends TestCase{

  private static JobConf job = new JobConf();
  private static final Log LOG = LogFactory.getLog(TestMultiFileInputFormat.class);
  
  private static final int MAX_SPLIT_COUNT  = 10000;
  private static final int SPLIT_COUNT_INCR = 6000;
  private static final int MAX_BYTES = 1024;
  private static final int MAX_NUM_FILES = 10000;
  private static final int NUM_FILES_INCR = 8000;
  
  private Random rand = new Random(System.currentTimeMillis());
  private HashMap<String, Long> lengths = new HashMap<String, Long>();
  
  /** Dummy class to extend MultiFileInputFormat*/
  private class DummyMultiFileInputFormat extends MultiFileInputFormat {
    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job
        , Reporter reporter) throws IOException {
      return null;
    }
  }
  
  private Path initFiles(FileSystem fs, int numFiles) throws IOException{
    Path dir = new Path(System.getProperty("test.build.data",".") + "/mapred");
    Path multiFileDir = new Path(dir, "test.multifile");
    fs.delete(multiFileDir);
    fs.mkdirs(multiFileDir);
    LOG.info("Creating " + numFiles + " file(s) in " + multiFileDir);
    for(int i=0; i<numFiles ;i++) {
      Path path = new Path(multiFileDir, "file_" + i);
       FSDataOutputStream out = fs.create(path);
       int numBytes = rand.nextInt(MAX_BYTES);
       for(int j=0; j< numBytes; j++) {
         out.write(rand.nextInt());
       }
       out.close();
       if(LOG.isDebugEnabled()) {
         LOG.debug("Created file " + path + " with length " + numBytes);
       }
       lengths.put(path.getName(), new Long(numBytes));
    }
    job.setInputPath(multiFileDir);
    return multiFileDir;
  }
  
  public void testFormat() throws IOException {
    if(LOG.isInfoEnabled()) {
      LOG.info("Test started");
      LOG.info("Max split count           = " + MAX_SPLIT_COUNT);
      LOG.info("Split count increment     = " + SPLIT_COUNT_INCR);
      LOG.info("Max bytes per file        = " + MAX_BYTES);
      LOG.info("Max number of files       = " + MAX_NUM_FILES);
      LOG.info("Number of files increment = " + NUM_FILES_INCR);
    }
    
    MultiFileInputFormat format = new DummyMultiFileInputFormat();
    FileSystem fs = FileSystem.getLocal(job);
    
    for(int numFiles = 1; numFiles< MAX_NUM_FILES ; 
      numFiles+= (NUM_FILES_INCR / 2) + rand.nextInt(NUM_FILES_INCR / 2)) {
      
      Path dir = initFiles(fs, numFiles);
      BitSet bits = new BitSet(numFiles);
      for(int i=1;i< MAX_SPLIT_COUNT ;i+= rand.nextInt(SPLIT_COUNT_INCR) + 1) {
        LOG.info("Running for Num Files=" + numFiles + ", split count=" + i);
        
        MultiFileSplit[] splits = (MultiFileSplit[])format.getSplits(job, i);
        bits.clear();
        
        for(MultiFileSplit split : splits) {
          long splitLength = 0;
          for(Path p : split.getPaths()) {
            long length = fs.getContentLength(p);
            assertEquals(length, lengths.get(p.getName()).longValue());
            splitLength += length;
            String name = p.getName();
            int index = Integer.parseInt(
                name.substring(name.lastIndexOf("file_") + 5));
            assertFalse(bits.get(index));
            bits.set(index);
          }
          assertEquals(splitLength, split.getLength());
        }
      }
      assertEquals(bits.cardinality(), numFiles);
      fs.delete(dir);
    }
    LOG.info("Test Finished");
  }
  
  public static void main(String[] args) throws Exception{
    TestMultiFileInputFormat test = new TestMultiFileInputFormat();
    test.testFormat();
  }
}
