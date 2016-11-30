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
package org.apache.hadoop.io.erasurecode;

/**
 * Constants related to the erasure code feature.
 */
public final class ErasureCodeConstants {

  private ErasureCodeConstants() {
  }

  public static final String RS_DEFAULT_CODEC_NAME = "rs-default";
  public static final String RS_LEGACY_CODEC_NAME = "rs-legacy";
  public static final String XOR_CODEC_NAME = "xor";
  public static final String HHXOR_CODEC_NAME = "hhxor";

  public static final ECSchema RS_6_3_SCHEMA = new ECSchema(
      RS_DEFAULT_CODEC_NAME, 6, 3);

  public static final ECSchema RS_3_2_SCHEMA = new ECSchema(
      RS_DEFAULT_CODEC_NAME, 3, 2);

  public static final ECSchema RS_6_3_LEGACY_SCHEMA = new ECSchema(
      RS_LEGACY_CODEC_NAME, 6, 3);
}
