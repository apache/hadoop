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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodecOptions;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.coder.ErasureDecoder;
import org.apache.hadoop.io.erasurecode.coder.ErasureEncoder;
import org.apache.hadoop.io.erasurecode.grouper.BlockGrouper;

/**
 * Abstract Erasure Codec is defines the interface of each actual erasure
 * codec classes.
 */
@InterfaceAudience.Private
public abstract class ErasureCodec {

  private ECSchema schema;
  private ErasureCodecOptions codecOptions;
  private ErasureCoderOptions coderOptions;

  public ErasureCodec(Configuration conf,
                      ErasureCodecOptions options) {
    this.schema = options.getSchema();
    this.codecOptions = options;
    boolean allowChangeInputs = false;
    this.coderOptions = new ErasureCoderOptions(schema.getNumDataUnits(),
        schema.getNumParityUnits(), allowChangeInputs, false);
  }

  public String getName() {
    return schema.getCodecName();
  }

  public ECSchema getSchema() {
    return schema;
  }

  /**
   * Get a {@link ErasureCodecOptions}.
   * @return erasure codec options
   */
  public ErasureCodecOptions getCodecOptions() {
    return codecOptions;
  }

  protected void setCodecOptions(ErasureCodecOptions options) {
    this.codecOptions = options;
    this.schema = options.getSchema();
  }

  /**
   * Get a {@link ErasureCoderOptions}.
   * @return erasure coder options
   */
  public ErasureCoderOptions getCoderOptions() {
    return coderOptions;
  }

  protected void setCoderOptions(ErasureCoderOptions options) {
    this.coderOptions = options;
  }

  public abstract ErasureEncoder createEncoder();

  public abstract ErasureDecoder createDecoder();

  public BlockGrouper createBlockGrouper() {
    BlockGrouper blockGrouper = new BlockGrouper();
    blockGrouper.setSchema(getSchema());

    return blockGrouper;
  }
}
