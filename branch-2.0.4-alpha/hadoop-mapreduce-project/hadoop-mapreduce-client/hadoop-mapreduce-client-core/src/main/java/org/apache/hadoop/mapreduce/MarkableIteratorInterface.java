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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <code>MarkableIteratorInterface</code> is an interface for a iterator that 
 * supports mark-reset functionality. 
 *
 * <p>Mark can be called at any point during the iteration process and a reset
 * will go back to the last record before the call to the previous mark.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
interface MarkableIteratorInterface<VALUE> extends Iterator<VALUE> {
  /**
   * Mark the current record. A subsequent call to reset will rewind
   * the iterator to this record.
   * @throws IOException
   */
  void mark() throws IOException;
  
  /**
   * Reset the iterator to the last record before a call to the previous mark
   * @throws IOException
   */
  void reset() throws IOException;
  
  /**
   * Clear any previously set mark
   * @throws IOException
   */
  void clearMark() throws IOException;
}
