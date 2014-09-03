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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.nativetask.buffer.DataInputStream;
import org.apache.hadoop.mapred.nativetask.buffer.DataOutputStream;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;

/**
 * serializes key-value pair
 */
@InterfaceAudience.Private
public interface IKVSerializer {

  /**
   * update the length field of SizedWritable
   */
  public void updateLength(SizedWritable<?> key, SizedWritable<?> value) throws IOException;

  public int serializeKV(DataOutputStream out, SizedWritable<?> key,
      SizedWritable<?> value) throws IOException;

  public int serializePartitionKV(DataOutputStream out, int partitionId,
      SizedWritable<?> key, SizedWritable<?> value)
      throws IOException;

  public int deserializeKV(DataInputStream in, SizedWritable<?> key, SizedWritable<?> value)
    throws IOException;
}
