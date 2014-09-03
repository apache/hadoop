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
package org.apache.hadoop.mapred.nativetask.buffer;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public abstract class DataOutputStream extends OutputStream implements DataOutput {
  /**
   * Check whether this buffer has enough space to store length of bytes
   * 
   * @param length length of bytes
   */
  public abstract boolean shortOfSpace(int length) throws IOException;

  /**
   * Check whether there is unflushed data stored in the stream
   */
  public abstract boolean hasUnFlushedData();
}
