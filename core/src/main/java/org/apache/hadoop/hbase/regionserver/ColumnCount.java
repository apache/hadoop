/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

/**
 * Simple wrapper for a byte buffer and a counter.  Does not copy.
 * <p>
 * NOT thread-safe because it is not used in a multi-threaded context, yet.
 */
public class ColumnCount {
  private byte [] bytes;
  private int offset;
  private int length;
  private int count;
  
  /**
   * Constructor
   * @param column the qualifier to count the versions for
   */
  public ColumnCount(byte [] column) {
    this(column, 0);
  }
  
  /**
   * Constructor
   * @param column the qualifier to count the versions for
   * @param count initial count
   */
  public ColumnCount(byte [] column, int count) {
    this(column, 0, column.length, count);
  }
  
  /**
   * Constuctor
   * @param column the qualifier to count the versions for
   * @param offset in the passed buffer where to start the qualifier from
   * @param length of the qualifier
   * @param count initial count
   */
  public ColumnCount(byte [] column, int offset, int length, int count) {
    this.bytes = column;
    this.offset = offset;
    this.length = length;
    this.count = count;
  }
  
  /**
   * @return the buffer
   */
  public byte [] getBuffer(){
    return this.bytes;
  }
  
  /**
   * @return the offset
   */
  public int getOffset(){
    return this.offset;
  }
  
  /**
   * @return the length
   */
  public int getLength(){
    return this.length;
  }  
  
  /**
   * Decrement the current version count
   * @return current count
   */
  public int decrement() {
    return --count;
  }

  /**
   * Increment the current version count
   * @return current count
   */
  public int increment() {
    return ++count;
  }
  
  /**
   * Check to see if needed to fetch more versions
   * @param max
   * @return true if more versions are needed, false otherwise
   */
  public boolean needMore(int max) {
    if(this.count < max) {
      return true;
    }
    return false;
  }
}
