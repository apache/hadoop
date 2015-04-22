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

import java.util.Map;
import java.util.TreeMap;

/**
 * This manages EC schemas predefined and activated in the system.
 * It loads customized schemas and syncs with persisted ones in
 * NameNode image.
 *
 * This class is instantiated by the FSNamesystem.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public final class ECSchemaManager {

  /**
   * TODO: HDFS-8095
   */
  private static final int DEFAULT_DATA_BLOCKS = 6;
  private static final int DEFAULT_PARITY_BLOCKS = 3;
  private static final String DEFAULT_CODEC_NAME = "rs";
  private static final String DEFAULT_SCHEMA_NAME = "RS-6-3";
  private static final ECSchema SYS_DEFAULT_SCHEMA =
      new ECSchema(DEFAULT_SCHEMA_NAME,
               DEFAULT_CODEC_NAME, DEFAULT_DATA_BLOCKS, DEFAULT_PARITY_BLOCKS);

  //We may add more later.
  private static ECSchema[] SYS_SCHEMAS = new ECSchema[] {
      SYS_DEFAULT_SCHEMA
  };

  /**
   * All active EC activeSchemas maintained in NN memory for fast querying,
   * identified and sorted by its name.
   */
  private final Map<String, ECSchema> activeSchemas;

  ECSchemaManager() {

    this.activeSchemas = new TreeMap<String, ECSchema>();
    for (ECSchema schema : SYS_SCHEMAS) {
      activeSchemas.put(schema.getSchemaName(), schema);
    }

    /**
     * TODO: HDFS-7859 persist into NameNode
     * load persistent schemas from image and editlog, which is done only once
     * during NameNode startup. This can be done here or in a separate method.
     */
  }

  /**
   * Get system defined schemas.
   * @return system schemas
   */
  public static ECSchema[] getSystemSchemas() {
    return SYS_SCHEMAS;
  }

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

    // schema name is the identifier.
    return SYS_DEFAULT_SCHEMA.getSchemaName().equals(schema.getSchemaName());
  }

  /**
   * Get all EC schemas that's available to use.
   * @return all EC schemas
   */
  public ECSchema[] getSchemas() {
    ECSchema[] results = new ECSchema[activeSchemas.size()];
    return activeSchemas.values().toArray(results);
  }

  /**
   * Get the EC schema specified by the schema name.
   * @param schemaName
   * @return EC schema specified by the schema name
   */
  public ECSchema getSchema(String schemaName) {
    return activeSchemas.get(schemaName);
  }

  /**
   * Clear and clean up
   */
  public void clear() {
    activeSchemas.clear();
  }
}
