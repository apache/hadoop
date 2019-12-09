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
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

/**
 * A raw erasure coder factory for dummy raw coders.
 */
@InterfaceAudience.Private
public class DummyRawErasureCoderFactory implements RawErasureCoderFactory {
  public static final String CODER_NAME = "dummy_dummy";

  @Override
  public RawErasureEncoder createEncoder(ErasureCoderOptions coderOptions) {
    return new DummyRawEncoder(coderOptions);
  }

  @Override
  public RawErasureDecoder createDecoder(ErasureCoderOptions coderOptions) {
    return new DummyRawDecoder(coderOptions);
  }

  @Override
  public String getCoderName() {
    return CODER_NAME;
  }

  @Override
  public String getCodecName() {
    return ErasureCodeConstants.DUMMY_CODEC_NAME;
  }
}
