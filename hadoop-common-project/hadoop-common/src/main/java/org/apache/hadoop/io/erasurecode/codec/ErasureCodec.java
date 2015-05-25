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
package org.apache.hadoop.io.erasurecode.codec;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.erasurecode.coder.ErasureCoder;
import org.apache.hadoop.io.erasurecode.grouper.BlockGrouper;

/**
 * Erasure Codec API that's to cover the essential specific aspects of a code.
 * Currently it cares only block grouper and erasure coder. In future we may
 * add more aspects here to make the behaviors customizable.
 */
public interface ErasureCodec extends Configurable {

  /**
   * Create block grouper
   * @return block grouper
   */
  public BlockGrouper createBlockGrouper();

  /**
   * Create Erasure Encoder
   * @return erasure encoder
   */
  public ErasureCoder createEncoder();

  /**
   * Create Erasure Decoder
   * @return erasure decoder
   */
  public ErasureCoder createDecoder();

}
