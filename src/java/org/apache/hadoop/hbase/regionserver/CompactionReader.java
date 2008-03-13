/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

/** Interface for generic reader for compactions */
interface CompactionReader {
  
  /**
   * Closes the reader
   * @throws IOException
   */
  public void close() throws IOException;
  
  /**
   * Get the next key/value pair
   * 
   * @param key
   * @param val
   * @return true if more data was returned
   * @throws IOException
   */
  public boolean next(WritableComparable key, Writable val)
  throws IOException;
  
  /**
   * Resets the reader
   * @throws IOException
   */
  public void reset() throws IOException;
}