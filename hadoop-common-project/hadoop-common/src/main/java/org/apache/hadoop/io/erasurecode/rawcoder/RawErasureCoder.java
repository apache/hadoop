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
import org.apache.hadoop.conf.Configurable;

/**
 * RawErasureCoder is a common interface for {@link RawErasureEncoder} and
 * {@link RawErasureDecoder} as both encoder and decoder share some properties.
 *
 * RawErasureCoder is part of ErasureCodec framework, where ErasureCoder is
 * used to encode/decode a group of blocks (BlockGroup) according to the codec
 * specific BlockGroup layout and logic. An ErasureCoder extracts chunks of
 * data from the blocks and can employ various low level RawErasureCoders to
 * perform encoding/decoding against the chunks.
 *
 * To distinguish from ErasureCoder, here RawErasureCoder is used to mean the
 * low level constructs, since it only takes care of the math calculation with
 * a group of byte buffers.
 */
@InterfaceAudience.Private
public interface RawErasureCoder extends Configurable {

  /**
   * Get a coder option value.
   * @param option
   * @return
   */
  public Object getCoderOption(CoderOption option);

  /**
   * Set a coder option value.
   * @param option
   * @param value
   */
  public void setCoderOption(CoderOption option, Object value);

  /**
   * The number of data input units for the coding. A unit can be a byte,
   * chunk or buffer or even a block.
   * @return count of data input units
   */
  public int getNumDataUnits();

  /**
   * The number of parity output units for the coding. A unit can be a byte,
   * chunk, buffer or even a block.
   * @return count of parity output units
   */
  public int getNumParityUnits();

  /**
   * Should be called when release this coder. Good chance to release encoding
   * or decoding buffers
   */
  public void release();
}
