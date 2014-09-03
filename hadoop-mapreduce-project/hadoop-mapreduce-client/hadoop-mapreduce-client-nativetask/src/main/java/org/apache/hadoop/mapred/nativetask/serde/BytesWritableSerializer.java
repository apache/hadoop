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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.nativetask.INativeComparable;

@InterfaceAudience.Private
public class BytesWritableSerializer
  implements INativeComparable, INativeSerializer<BytesWritable> {

  @Override
  public int getLength(BytesWritable w) throws IOException {
    return w.getLength();
  }

  @Override
  public void serialize(BytesWritable w, DataOutput out) throws IOException {
    out.write(w.getBytes(), 0, w.getLength());
  }

  @Override
  public void deserialize(DataInput in, int length, BytesWritable w) throws IOException {
    w.setSize(length);
    in.readFully(w.getBytes(), 0, length);
  }
}