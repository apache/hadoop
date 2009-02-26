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

import java.util.regex.*;
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

/***
 * An InputFormat for processing logfiles in Chukwa.
 * Designed to be a nearly drop-in replacement for the Hadoop default
 * TextInputFormat so that code can be ported to use Chukwa with minimal
 * modification.
 *
 * Has an optional configuration option, chukwa.inputfilter.datatype
 * which can be used to filter the input by datatype. If need
 * exists, this mechanism could be extended to also filter by
 * other fields.
 * 
 */
public class ChukwaInputFormat extends SequenceFileInputFormat<LongWritable, Text> {

  public static class ChukwaRecordReader implements RecordReader<LongWritable, Text> {
    
    static Logger LOG = Logger.getLogger(ChukwaInputFormat.class);
    
    private SequenceFileRecordReader<ChukwaArchiveKey, Chunk> sfrr;
    private long lineInFile =0;
    private Chunk curChunk = null;
    private int lineInChunk; //outside of next, it's the array offset of next line to be returned
    private int[] lineOffsets = null;
    private int byteOffsetOfLastLine = 0;
    Pattern dtPattern;
    
    
    public ChukwaRecordReader(Configuration conf, FileSplit split)
    throws IOException {
      sfrr = new SequenceFileRecordReader<ChukwaArchiveKey, Chunk>(conf, split);
      dtPattern = Pattern.compile(conf.get("chukwa.inputfilter.datatype", ".*"));
    }
  
    @Override
    public void close() throws IOException {
      sfrr.close();
    }
  
    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }
  
    @Override
    public Text createValue() {
      return new Text();
    }
  
    @Override
    public long getPos() throws IOException {
      return sfrr.getPos();
    }
  
    @Override
    public float getProgress() throws IOException {
      return sfrr.getProgress();
    }
  
    private boolean passesFilters(Chunk c) {
      return dtPattern.matcher(c.getDataType()).matches();
    }
    
    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
      if(curChunk == null) {
        ChukwaArchiveKey k = new ChukwaArchiveKey();
        curChunk = ChunkImpl.getBlankChunk();
        boolean unfilteredChunk = false;
        while(!unfilteredChunk) {
          boolean readOK = sfrr.next(k, curChunk);
          if(!readOK) {
            curChunk = null;
            return false;
          }
          unfilteredChunk = passesFilters(curChunk);
        }
        lineOffsets = curChunk.getRecordOffsets();
        lineInChunk = 0;
        byteOffsetOfLastLine = 0;
      } //end curChunk == null
      value.set(curChunk.getData(), byteOffsetOfLastLine , lineOffsets[lineInChunk]-byteOffsetOfLastLine);
      if(lineInChunk >= lineOffsets.length - 1) { //end of chunk
        curChunk = null;
      } else
        byteOffsetOfLastLine = lineOffsets[lineInChunk++]+1;
      
      key.set(lineInFile);
      lineInFile++;
      return true;
    }
  } //end ChukwaRecordReader

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter)
  throws IOException {
    reporter.setStatus(split.toString());
    LOG.info("returning a new chukwa record reader");
    return new ChukwaRecordReader(job, (FileSplit) split);
  }

}
