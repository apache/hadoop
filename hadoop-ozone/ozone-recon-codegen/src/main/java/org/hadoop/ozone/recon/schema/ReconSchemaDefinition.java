/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hadoop.ozone.recon.schema;

import java.sql.SQLException;

/**
 * Classes meant to initialize the SQL schema for Recon. The implementations of
 * this class will be used to create the SQL schema programmatically.
 * Note: Make sure add a binding for your implementation to the Guice module,
 * otherwise code-generator will not pick up the schema changes.
 */
public interface ReconSchemaDefinition {

  /**
   * Execute DDL that will create Recon schema.
   */
  void initializeSchema() throws SQLException;
}
