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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECSchema;

/**
 * This manages EC schemas predefined and activated in the system. It loads from
 * predefined ones in XML and syncs with persisted ones in NameNode image.
 *
 * This class is instantiated by the FSNamesystem.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class ECSchemaManager {

  private static final int DEFAULT_DATA_BLOCKS = 6;
  private static final int DEFAULT_PARITY_BLOCKS = 3;
  private static final String DEFAULT_CODEC_NAME = "rs";
  private static final String DEFAULT_SCHEMA_NAME = "SYS-DEFAULT-RS-6-3";

  private static ECSchema SYS_DEFAULT_SCHEMA = new ECSchema(DEFAULT_SCHEMA_NAME,
      DEFAULT_CODEC_NAME, DEFAULT_DATA_BLOCKS, DEFAULT_PARITY_BLOCKS);

  /**
   * Get system-wide default EC schema, which can be used by default when no
   * schema is specified for an EC zone.
   * @return schema
   */
  public static ECSchema getSystemDefaultSchema() {
    return SYS_DEFAULT_SCHEMA;
  }

  /**
   * Tell the specified schema is the system default one or not.
   * @param schema
   * @return true if it's the default false otherwise
   */
  public static boolean isSystemDefault(ECSchema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Invalid schema parameter");
    }

    // schema name is the identifier, but for safety we check all properties.
    return SYS_DEFAULT_SCHEMA.equals(schema);
  }
}
