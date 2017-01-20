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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * contains the constants used in the context of schema accesses for
 * {@link org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity}
 * information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class TimelineHBaseSchemaConstants {
  private TimelineHBaseSchemaConstants() {
  }

  /**
   * Used to create a pre-split for tables starting with a username in the
   * prefix. TODO: this may have to become a config variable (string with
   * separators) so that different installations can presplit based on their own
   * commonly occurring names.
   */
  private final static byte[][] USERNAME_SPLITS = {
      Bytes.toBytes("a"), Bytes.toBytes("ad"), Bytes.toBytes("an"),
      Bytes.toBytes("b"), Bytes.toBytes("ca"), Bytes.toBytes("cl"),
      Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
      Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i"),
      Bytes.toBytes("j"), Bytes.toBytes("k"), Bytes.toBytes("l"),
      Bytes.toBytes("m"), Bytes.toBytes("n"), Bytes.toBytes("o"),
      Bytes.toBytes("q"), Bytes.toBytes("r"), Bytes.toBytes("s"),
      Bytes.toBytes("se"), Bytes.toBytes("t"), Bytes.toBytes("u"),
      Bytes.toBytes("v"), Bytes.toBytes("w"), Bytes.toBytes("x"),
      Bytes.toBytes("y"), Bytes.toBytes("z")
  };

  /**
   * The length at which keys auto-split.
   */
  public static final String USERNAME_SPLIT_KEY_PREFIX_LENGTH = "4";

  /**
   * @return splits for splits where a user is a prefix.
   */
  public static byte[][] getUsernameSplits() {
    byte[][] kloon = USERNAME_SPLITS.clone();
    // Deep copy.
    for (int row = 0; row < USERNAME_SPLITS.length; row++) {
      kloon[row] = Bytes.copy(USERNAME_SPLITS[row]);
    }
    return kloon;
  }

}