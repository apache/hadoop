/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import org.apache.hadoop.io.Text;

/**
 * HScannerInterface iterates through a set of rows.  It's implemented by
 * several classes.  Implements {@link Iterable} but be sure to still call
 * {@link #close()} when done with your {@link Iterator}
 */
public interface HScannerInterface extends Closeable,
Iterable<Map.Entry<HStoreKey, SortedMap<Text, byte []>>> {
  /**
   * Grab the next row's worth of values. The scanner will return the most
   * recent data value for each row that is not newer than the target time
   * passed when the scanner was created.
   * @param key will contain the row and timestamp upon return
   * @param results will contain an entry for each column family member and its
   * value
   * @return true if data was returned
   * @throws IOException
   */
  public boolean next(HStoreKey key, SortedMap<Text, byte[]> results)
  throws IOException;
  
  /**
   * Closes a scanner and releases any resources it has allocated
   * @throws IOException
   */
  public void close() throws IOException;
}
