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
 * Raw erasure coder factory that can be used to create raw encoder and decoder.
 * It helps in configuration since only one factory class is needed to be
 * configured.
 */
@InterfaceAudience.Private
public interface RawErasureCoderFactory {

  /**
   * Create raw erasure encoder.
   * @param coderOptions the options used to create the encoder
   * @return raw erasure encoder
   */
  RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions);

  /**
   * Create raw erasure decoder.
   * @param coderOptions the options used to create the encoder
   * @return raw erasure decoder
   */
  RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions);

  /**
   * Get the name of the coder.
   * @return coder name
   */
  String getCoderName();

  /**
   * Get the name of its codec.
   * @return codec name
   */
  String getCodecName();
}
