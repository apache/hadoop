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

/** Singleton Writable with no data. */
public class NullWritable implements WritableComparable {

  private static final NullWritable THIS = new NullWritable();

  private NullWritable() {}                       // no public ctor

  /** Returns the single instance of this class. */
  public static NullWritable get() { return THIS; }
  
  public String toString() {
    return "(null)";
  }

  public int hashCode() { return 0; }
  public int compareTo(Object other) {
    if (!(other instanceof NullWritable)) {
      throw new ClassCastException("can't compare " + other.getClass().getName() 
                                   + " to NullWritable");
    }
    return 0;
  }
  public boolean equals(Object other) { return other instanceof NullWritable; }
  public void readFields(DataInput in) throws IOException {}
  public void write(DataOutput out) throws IOException {}
}

