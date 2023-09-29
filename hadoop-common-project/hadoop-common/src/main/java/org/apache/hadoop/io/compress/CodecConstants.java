/*
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

package org.apache.hadoop.io.compress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Codec related constants.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class CodecConstants {

  private CodecConstants() {
  }
  /**
   * Default extension for {@link org.apache.hadoop.io.compress.DefaultCodec}.
   */
  public static final String DEFAULT_CODEC_EXTENSION = ".deflate";

  /**
   * Default extension for {@link org.apache.hadoop.io.compress.BZip2Codec}.
   */
  public static final String BZIP2_CODEC_EXTENSION = ".bz2";

  /**
   * Default extension for {@link org.apache.hadoop.io.compress.GzipCodec}.
   */
  public static final String GZIP_CODEC_EXTENSION = ".gz";

  /**
   * Default extension for {@link org.apache.hadoop.io.compress.Lz4Codec}.
   */
  public static final String LZ4_CODEC_EXTENSION = ".lz4";

  /**
   * Default extension for
   * {@link org.apache.hadoop.io.compress.PassthroughCodec}.
   */
  public static final String PASSTHROUGH_CODEC_EXTENSION = ".passthrough";

  /**
   * Default extension for {@link org.apache.hadoop.io.compress.SnappyCodec}.
   */
  public static final String SNAPPY_CODEC_EXTENSION = ".snappy";

  /**
   * Default extension for {@link org.apache.hadoop.io.compress.ZStandardCodec}.
   */
  public static final String ZSTANDARD_CODEC_EXTENSION = ".zst";
}
