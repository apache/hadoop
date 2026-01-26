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
package org.apache.hadoop.yarn.server.federation.store.sql;

import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * SQLOutParameter is used to set the output parameters of the stored procedure.
 * @param <T> Generic T.
 */
public class FederationSQLOutParameter<T> {
  private final int sqlType;
  private final Class<T> javaType;
  private T value = null;
  private String paramName;

  public FederationSQLOutParameter(String paramName, int sqlType, Class<T> javaType) {
    this.paramName = paramName;
    this.sqlType = sqlType;
    this.javaType = javaType;
  }

  public FederationSQLOutParameter(int sqlType, Class<T> javaType, T value) {
    this.sqlType = sqlType;
    this.javaType = javaType;
    this.value = value;
  }

  public int getSqlType() {
    return sqlType;
  }

  public Class<T> getJavaType() {
    return javaType;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public String getParamName() {
    return paramName;
  }

  public void setParamName(String paramName) {
    this.paramName = paramName;
  }

  void setValue(CallableStatement stmt, int index) throws SQLException {
    Object object = stmt.getObject(index);
    value = javaType.cast(object);
  }

  void register(CallableStatement stmt, int index) throws SQLException {
    stmt.registerOutParameter(index, sqlType);
    if (value != null) {
      stmt.setObject(index, value);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("OutParameter: [")
        .append("SqlType: ").append(sqlType).append(", ")
        .append("JavaType: ").append(javaType).append(", ")
        .append("Value: ").append(value)
        .append("]");
    return sb.toString();
  }
}
