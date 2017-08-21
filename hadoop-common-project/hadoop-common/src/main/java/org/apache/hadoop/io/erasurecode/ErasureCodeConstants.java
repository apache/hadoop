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

  public static final String DUMMY_CODEC_NAME = "dummy";
  public static final String RS_CODEC_NAME = "rs";
  public static final String RS_LEGACY_CODEC_NAME = "rs-legacy";
  public static final String XOR_CODEC_NAME = "xor";
  public static final String HHXOR_CODEC_NAME = "hhxor";
  public static final String REPLICATION_CODEC_NAME = "replication";

  public static final ECSchema RS_6_3_SCHEMA = new ECSchema(
      RS_CODEC_NAME, 6, 3);

  public static final ECSchema RS_3_2_SCHEMA = new ECSchema(
      RS_CODEC_NAME, 3, 2);

  public static final ECSchema RS_6_3_LEGACY_SCHEMA = new ECSchema(
      RS_LEGACY_CODEC_NAME, 6, 3);

  public static final ECSchema XOR_2_1_SCHEMA = new ECSchema(
      XOR_CODEC_NAME, 2, 1);

  public static final ECSchema RS_10_4_SCHEMA = new ECSchema(
      RS_CODEC_NAME, 10, 4);

  public static final ECSchema REPLICATION_1_2_SCHEMA = new ECSchema(
      REPLICATION_CODEC_NAME, 1, 2);

  public static final byte USER_DEFINED_POLICY_START_ID = (byte) 64;
  public static final byte REPLICATION_POLICY_ID = (byte) 63;
  public static final String REPLICATION_POLICY_NAME = REPLICATION_CODEC_NAME;
}
