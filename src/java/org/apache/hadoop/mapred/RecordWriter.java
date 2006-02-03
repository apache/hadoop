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
import java.io.DataOutput;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

/** Writes key/value pairs to an output file.  Implemented by {@link
 * OutputFormat} implementations. */
public interface RecordWriter {
  /** Writes a key/value pair.
   *
   * @param key the key to write
   * @param value the value to write
   *
   * @see Writable#write(DataOutput)
   */      
  void write(WritableComparable key, Writable value) throws IOException;

  /** Close this to future operations.*/ 
  void close(Reporter reporter) throws IOException;
}
