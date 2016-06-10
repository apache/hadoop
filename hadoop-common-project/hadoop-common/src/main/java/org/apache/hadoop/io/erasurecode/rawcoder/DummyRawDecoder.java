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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

/**
 * A dummy raw decoder that does no real computation.
 * Instead, it just returns zero bytes.
 * This decoder can be used to isolate the performance issue to HDFS side logic
 * instead of codec, and is intended for test only.
 */
@InterfaceAudience.Private
public class DummyRawDecoder extends RawErasureDecoder {

  public DummyRawDecoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);
  }

  @Override
  protected void doDecode(ByteBufferDecodingState decodingState) {
    // Nothing to do. Output buffers have already been reset
  }

  @Override
  protected void doDecode(ByteArrayDecodingState decodingState) {
    // Nothing to do. Output buffers have already been reset
  }
}
