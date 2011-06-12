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

package org.apache.hadoop.sqoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Types;

/**
 * Defines conversion between SQL types and Hive types.
 */
public class HiveTypes {

  public static final Log LOG = LogFactory.getLog(HiveTypes.class.getName());

  /**
   * Given JDBC SQL types coming from another database, what is the best
   * mapping to a Hive-specific type?
   */
  public static String toHiveType(int sqlType) {
    if (sqlType == Types.INTEGER) {
      return "INT";
    } else if (sqlType == Types.VARCHAR) {
      return "STRING";
    } else if (sqlType == Types.CHAR) {
      return "STRING";
    } else if (sqlType == Types.LONGVARCHAR) {
      return "STRING";
    } else if (sqlType == Types.NUMERIC) {
      // Per suggestion on hive-user, this is converted to DOUBLE for now.
      return "DOUBLE";
    } else if (sqlType == Types.DECIMAL) {
      // Per suggestion on hive-user, this is converted to DOUBLE for now.
      return "DOUBLE";
    } else if (sqlType == Types.BIT) {
      return "BOOLEAN";
    } else if (sqlType == Types.BOOLEAN) {
      return "BOOLEAN";
    } else if (sqlType == Types.TINYINT) {
      return "TINYINT";
    } else if (sqlType == Types.SMALLINT) {
      return "INTEGER";
    } else if (sqlType == Types.BIGINT) {
      return "BIGINT";
    } else if (sqlType == Types.REAL) {
      return "DOUBLE";
    } else if (sqlType == Types.FLOAT) {
      return "DOUBLE";
    } else if (sqlType == Types.DOUBLE) {
      return "DOUBLE";
    } else if (sqlType == Types.DATE) {
      // unfortunate type coercion
      return "STRING";
    } else if (sqlType == Types.TIME) {
      // unfortunate type coercion
      return "STRING";
    } else if (sqlType == Types.TIMESTAMP) {
      // unfortunate type coercion
      return "STRING";
    } else {
      // TODO(aaron): Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB,
      // BLOB, ARRAY, STRUCT, REF, JAVA_OBJECT.
      return null;
    }
  }

  /** 
   * @return true if a sql type can't be translated to a precise match
   * in Hive, and we have to cast it to something more generic.
   */
  public static boolean isHiveTypeImprovised(int sqlType) {
    return sqlType == Types.DATE || sqlType == Types.TIME
        || sqlType == Types.TIMESTAMP
        || sqlType == Types.DECIMAL
        || sqlType == Types.NUMERIC;
  }
}

