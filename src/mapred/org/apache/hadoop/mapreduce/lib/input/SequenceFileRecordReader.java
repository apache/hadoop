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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** An {@link RecordReader} for {@link SequenceFile}s. */
public class SequenceFileRecordReader<K, V> extends RecordReader<K, V> {
  private SequenceFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  protected Configuration conf;
  
  @Override
  public void initialize(InputSplit split, 
                         TaskAttemptContext context
                         ) throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    conf = context.getConfiguration();    
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    this.in = new SequenceFile.Reader(fs, path, conf);
    this.end = fileSplit.getStart() + fileSplit.getLength();

    if (fileSplit.getStart() > in.getPosition()) {
      in.sync(fileSplit.getStart());                  // sync to start
    }

    this.start = in.getPosition();
    more = start < end;
  }

  @Override
  @SuppressWarnings("unchecked")
  public K nextKey(K key) throws IOException, InterruptedException {
    if (!more) {
      return null;
    }
    long pos = in.getPosition();
    K result = (K) in.next(key);
    if (result == null || (pos >= end && in.syncSeen())) {
      more = false;
      result = null;
    }
    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public V nextValue(V value) throws IOException, InterruptedException {
    return (V) in.getCurrentValue(value);
  }
  
  /**
   * Return the progress within the input split
   * @return 0.0 to 1.0 of the input byte range
   */
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException { in.close(); }
  
}

