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

package org.apache.hadoop.hive.ql.metadata;

/**                                                                                     
 * Generic exception class for Hive
 *                                                                                      
 */

public class InvalidTableException extends HiveException {
  String tableName;

  public InvalidTableException(String tableName) {
    super();
    this.tableName = tableName;
  }
  
  public InvalidTableException(String message, String tableName) {
    super(message);
    this.tableName = tableName;
  }

  public InvalidTableException(Throwable cause, String tableName) {
    super(cause);
    this.tableName = tableName;
  }

  public InvalidTableException(String message, Throwable cause, String tableName) {
    super(message, cause);
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

}
