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
package org.apache.hadoop.hdfs.util;

import java.util.Arrays;

/** 
 * Wrapper for byte[] to use byte[] as key in HashMap
 */
public class ByteArray {
  private int hash = 0; // cache the hash code
  private final byte[] bytes;
  
  public ByteArray(byte[] bytes) {
    this.bytes = bytes;
  }
  
  public byte[] getBytes() {
    return bytes;
  }
  
  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Arrays.hashCode(bytes);
    }
    return hash;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ByteArray)) {
      return false;
    }
    return Arrays.equals(bytes, ((ByteArray)o).bytes);
  }
}
