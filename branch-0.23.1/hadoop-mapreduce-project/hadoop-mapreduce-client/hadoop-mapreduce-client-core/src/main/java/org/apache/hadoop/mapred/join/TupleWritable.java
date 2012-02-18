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

package org.apache.hadoop.mapred.join;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/**
 * Writable type storing multiple {@link org.apache.hadoop.io.Writable}s.
 *
 * This is *not* a general-purpose tuple type. In almost all cases, users are
 * encouraged to implement their own serializable types, which can perform
 * better validation and provide more efficient encodings than this class is
 * capable. TupleWritable relies on the join framework for type safety and
 * assumes its instances will rarely be persisted, assumptions not only
 * incompatible with, but contrary to the general case.
 *
 * @see org.apache.hadoop.io.Writable
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TupleWritable 
    extends org.apache.hadoop.mapreduce.lib.join.TupleWritable {

  /**
   * Create an empty tuple with no allocated storage for writables.
   */
  public TupleWritable() {
    super();
  }

  /**
   * Initialize tuple with storage; unknown whether any of them contain
   * &quot;written&quot; values.
   */
  public TupleWritable(Writable[] vals) {
    super(vals);
  }

  /**
   * Record that the tuple contains an element at the position provided.
   */
  void setWritten(int i) {
    written.set(i);
  }

  /**
   * Record that the tuple does not contain an element at the position
   * provided.
   */
  void clearWritten(int i) {
    written.clear(i);
  }

  /**
   * Clear any record of which writables have been written to, without
   * releasing storage.
   */
  void clearWritten() {
    written.clear();
  }


}
