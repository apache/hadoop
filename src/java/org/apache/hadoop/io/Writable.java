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

package org.apache.hadoop.io;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

/** A simple, efficient, serialization protocol, based on {@link DataInput} and
 * {@link DataOutput}.
 *
 * <p>Implementations typically implement a static <code>read(DataInput)</code>
 * method which constructs a new instance, calls {@link
 * #readFields(DataInput)}, and returns the instance.
 *
 * @author Doug Cutting
 */
public interface Writable {
  /** Writes the fields of this object to <code>out</code>. */
  void write(DataOutput out) throws IOException;

  /** Reads the fields of this object from <code>in</code>.  For efficiency,
   * implementations should attempt to re-use storage in the existing object
   * where possible.
   */
  void readFields(DataInput in) throws IOException;
}
