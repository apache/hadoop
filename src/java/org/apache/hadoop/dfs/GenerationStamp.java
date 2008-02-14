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
package org.apache.hadoop.dfs;

import java.io.*;
import org.apache.hadoop.io.*;

/****************************************************************
 * A GenerationStamp is a Hadoop FS primitive, identified by a long.
 ****************************************************************/
class GenerationStamp implements WritableComparable {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (GenerationStamp.class,
       new WritableFactory() {
         public Writable newInstance() { return new GenerationStamp(); }
       });
  }

  long genstamp;

  /**
   * Create a new instance, initialized to 0.
   */
  public GenerationStamp() {
    this.genstamp = 0;
  }

  /**
   * Create a new instance, initialized to the specified value.
   */
  public GenerationStamp(long stamp) {
    this.genstamp = stamp;
  }

  /**
   * Returns the current generation stamp
   */
  public long getStamp() {
    return this.genstamp;
  }

  /**
   * Sets the current generation stamp
   */
  public void setStamp(long stamp) {
    this.genstamp = stamp;
  }

  /**
   * Returns the current stamp and incrments the counter
   */
  public long nextStamp() {
    this.genstamp++;
    return this.genstamp;
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeLong(genstamp);
  }

  public void readFields(DataInput in) throws IOException {
    this.genstamp = in.readLong();
    if (this.genstamp < 0) {
      throw new IOException("Bad Generation Stamp: " + this.genstamp);
    }
  }

  /////////////////////////////////////
  // Comparable
  /////////////////////////////////////
  public int compareTo(Object o) {
    GenerationStamp b = (GenerationStamp) o;
    if (genstamp < b.genstamp) {
      return -1;
    } else if (genstamp == b.genstamp) {
      return 0;
    } else {
      return 1;
    }
  }
  public boolean equals(Object o) {
    if (!(o instanceof GenerationStamp)) {
      return false;
    }
    return genstamp == ((GenerationStamp)o).genstamp;
  }
    
  public int hashCode() {
    return 37 * 17 + (int) (genstamp^(genstamp>>>32));
  }
}
