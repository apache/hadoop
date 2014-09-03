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

package org.apache.hadoop.mapred.nativetask.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * an INativeSerializer serializes and deserializes data transferred between
 * Java and native. {@link DefaultSerializer} provides default implementations.
 *
 * Note: if you implemented your customized NativeSerializer instead of DefaultSerializer,
 * you have to make sure the native side can serialize it correctly.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface INativeSerializer<T> {

  /**
   * get length of data to be serialized. If the data length is already known (like IntWritable)
   * and could immediately be returned from this method, it is good chance to implement customized
   * NativeSerializer for efficiency
   */
  public int getLength(T w) throws IOException;

  public void serialize(T w, DataOutput out) throws IOException;

  public void deserialize(DataInput in, int length, T w) throws IOException;
}
