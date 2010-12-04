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

package org.apache.hadoop.io;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;


/** A dense file-based mapping from integers to values. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayFile extends MapFile {

  protected ArrayFile() {}                            // no public ctor

  /** Write a new array file. */
  public static class Writer extends MapFile.Writer {
    private LongWritable count = new LongWritable(0);

    /** Create the named file for values of the named class. */
    public Writer(Configuration conf, FileSystem fs,
                  String file, Class<?> valClass)
      throws IOException {
      super(conf, new Path(file), keyClass(LongWritable.class), 
            valueClass(valClass));
    }

    /** Create the named file for values of the named class. */
    public Writer(Configuration conf, FileSystem fs,
                  String file, Class<?> valClass,
                  CompressionType compress, Progressable progress)
      throws IOException {
      super(conf, new Path(file), 
            keyClass(LongWritable.class), 
            valueClass(valClass), 
            compression(compress), 
            progressable(progress));
    }

    /** Append a value to the file. */
    public synchronized void append(Object value) throws IOException {
      super.append(count, value);                 // add to map
      count.set(count.get()+1);                   // increment count
    }
  }

  /** Provide access to an existing array file. */
  public static class Reader extends MapFile.Reader {
    private LongWritable key = new LongWritable();

    /** Construct an array reader for the named file.*/
    public Reader(FileSystem fs, String file, 
                  Configuration conf) throws IOException {
      super(new Path(file), conf);
    }

    /** Positions the reader before its <code>n</code>th value. */
    public synchronized void seek(long n) throws IOException {
      key.set(n);
      seek(key);
    }

    @Deprecated
    public synchronized Writable next(Writable value) throws IOException {
      return (Writable) next((Object) value);
    }

    /** Read and return the next value in the file. */
    public synchronized Object next(Object value) throws IOException {
      key = (LongWritable) nextKey(key);
      return key == null? null : getCurrentValue(value);
    }

    /** Returns the key associated with the most recent call to {@link
     * #seek(long)}, {@link #next(Object)}, or {@link
     * #get(long,Object)}. */
    public synchronized long key() throws IOException {
      return key.get();
    }

    @Deprecated
    public synchronized Writable get(long n, Writable value) throws IOException{
      return (Writable) get(n, (Object) value);
    }

    /** Return the <code>n</code>th value in the file. */
    public synchronized Object get(long n, Object value)
      throws IOException {
      key.set(n);
      return get(key, value);
    }
  }

}
