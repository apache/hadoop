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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

/** HiveKey is a simple wrapper on Text which allows us to set the hashCode easily.
 *  hashCode is used for hadoop partitioner.
 */ 
public class HiveKey extends BytesWritable {

  private static final int LENGTH_BYTES = 4;
  
  boolean hashCodeValid;
  public HiveKey() {
    hashCodeValid = false; 
  }
  
  protected int myHashCode; 
  public void setHashCode(int myHashCode) {
    this.hashCodeValid = true;
    this.myHashCode = myHashCode;
  }
  public int hashCode() {
    if (!hashCodeValid) {
      throw new RuntimeException("Cannot get hashCode() from deserialized " + HiveKey.class);
    }
    return myHashCode;
  }

  /** A Comparator optimized for HiveKey. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(HiveKey.class);
    }
    
    /**
     * Compare the buffers in serialized form.
     */
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1+LENGTH_BYTES, l1-LENGTH_BYTES, 
                          b2, s2+LENGTH_BYTES, l2-LENGTH_BYTES);
    }
  }
  
  static {
    WritableComparator.define(HiveKey.class, new Comparator());
  }
}
