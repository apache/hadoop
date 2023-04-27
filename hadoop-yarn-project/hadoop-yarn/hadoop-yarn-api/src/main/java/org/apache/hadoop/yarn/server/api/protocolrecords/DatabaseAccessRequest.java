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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;


/**
 * The request used by Admins to access the underlying RM database
 * The request contains details of the operation (get / set / delete), database (RMStateStore or YarnConfigurationStore), key and value
 */
public abstract class DatabaseAccessRequest {

  /**
   *
   * @param operation - get / set / del
   * @param database - unique identifier for the database to query
   *
   * Can add dataStore later if required to access other datastores like ZK / mysql etc
   */
  public static DatabaseAccessRequest newInstance(String operation, String database, String key, String value) {
    DatabaseAccessRequest request = Records.newRecord(DatabaseAccessRequest.class);
    request.setOperation(operation);
    request.setDatabase(database);
    request.setKey(key);
    request.setValue(value);
    return request;
  }

  public abstract void setOperation(String operation);

  public abstract void setDatabase(String database);

  public abstract void setKey(String key);

  public abstract void setValue(String value);

  public abstract String getOperation();

  public abstract String getDatabase();

  public abstract String getKey();

  public abstract String getValue();
}
