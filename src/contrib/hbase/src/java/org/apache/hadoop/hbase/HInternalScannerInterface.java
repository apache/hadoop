/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

/**
 * Internally, we need to be able to determine if the scanner is doing wildcard
 * column matches (when only a column family is specified or if a column regex
 * is specified) or if multiple members of the same column family were
 * specified. If so, we need to ignore the timestamp to ensure that we get all
 * the family members, as they may have been last updated at different times.
 */
public interface HInternalScannerInterface {
  
  /**
   * Grab the next row's worth of values. The HScanner will return the most
   * recent data value for each row that is not newer than the target time.
   * 
   * If a dataFilter is defined, it will be used to skip rows that do not
   * match its criteria. It may cause the scanner to stop prematurely if it
   * knows that it will no longer accept the remaining results.
   * 
   * @param key HStoreKey containing row and timestamp
   * @param results Map of column/value pairs
   * @return true if a value was found
   * @throws IOException
   */
  public boolean next(HStoreKey key, TreeMap<Text, byte []> results)
  throws IOException;
  
  /**
   * Close the scanner.
   */
  public void close();
  
  /** @return true if the scanner is matching a column family or regex */
  public boolean isWildcardScanner();
  
  /** @return true if the scanner is matching multiple column family members */
  public boolean isMultipleMatchScanner();
}