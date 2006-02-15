/**
 * Copyright 2005 The Apache Software Foundation
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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/** Maps input key/value pairs to a set of intermediate key/value pairs.  All
 * intermediate values associated with a given output key are subsequently
 * grouped by the map/reduce system, and passed to a {@link Reducer} to
 * determine the final output.. */
public interface Mapper extends JobConfigurable, Closeable {
  /** Maps a single input key/value pair into intermediate key/value pairs.
   * Output pairs need not be of the same types as input pairs.  A given input
   * pair may map to zero or many output pairs.  Output pairs are collected
   * with calls to {@link
   * OutputCollector#collect(WritableComparable,Writable)}.
   *
   * @param key the key
   * @param value the values
   * @param output collects mapped keys and values
   */
  void map(WritableComparable key, Writable value,
           OutputCollector output, Reporter reporter)
    throws IOException;
}
