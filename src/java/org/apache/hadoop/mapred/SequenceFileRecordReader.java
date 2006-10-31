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

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/** An {@link RecordReader} for {@link SequenceFile}s. */
public class SequenceFileRecordReader implements RecordReader {
  private SequenceFile.Reader in;
  private long end;
  private boolean more = true;
  private Configuration conf;

  public SequenceFileRecordReader(Configuration conf, FileSplit split)
    throws IOException {
    FileSystem fs = FileSystem.get(conf);
    this.in = new SequenceFile.Reader(fs, split.getPath(), conf);
    this.end = split.getStart() + split.getLength();
    this.conf = conf;

    if (split.getStart() > in.getPosition())
      in.sync(split.getStart());                  // sync to start

    more = in.getPosition() < end;
  }


  /** The class of key that must be passed to {@link
   * #next(Writable,Writable)}.. */
  public Class getKeyClass() { return in.getKeyClass(); }

  /** The class of value that must be passed to {@link
   * #next(Writable,Writable)}.. */
  public Class getValueClass() { return in.getValueClass(); }
  
  public WritableComparable createKey() {
    return (WritableComparable) ReflectionUtils.newInstance(getKeyClass(), 
                                                            conf);
  }
  
  public Writable createValue() {
    return (Writable) ReflectionUtils.newInstance(getValueClass(), conf);
  }
    
  public synchronized boolean next(Writable key, Writable value)
    throws IOException {
    if (!more) return false;
    long pos = in.getPosition();
    boolean eof = in.next(key, value);
    if (pos >= end && in.syncSeen()) {
      more = false;
    } else {
      more = eof;
    }
    return more;
  }
  
  protected synchronized boolean next(Writable key)
      throws IOException {
      if (!more) return false;
      long pos = in.getPosition();
      boolean eof = in.next(key);
      if (pos >= end && in.syncSeen()) {
          more = false;
      } else {
          more = eof;
      }
      return more;
  }
  
  protected synchronized void getCurrentValue(Writable value)
      throws IOException {
      in.getCurrentValue(value);
  }
  
  public synchronized long getPos() throws IOException {
    return in.getPosition();
  }
  
  protected synchronized void seek(long pos) throws IOException {
      in.seek(pos);
  }
  public synchronized void close() throws IOException { in.close(); }
  
}

