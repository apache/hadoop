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

import org.apache.hadoop.util.StringUtils;

import java.sql.SQLException;

/**
 * RowCount Handler.
 * Used to parse out the rowCount information of the output parameter.
 */
public class RowCountHandler implements ResultSetHandler<Integer> {

  private String rowCountParamName;

  public RowCountHandler(String paramName) {
    this.rowCountParamName = paramName;
  }

  @Override
  public Integer handle(Object... params) throws SQLException {
    Integer result = 0;
    for (Object param : params) {
      if (param instanceof FederationSQLOutParameter) {
        FederationSQLOutParameter parameter = (FederationSQLOutParameter) param;
        String paramName = parameter.getParamName();
        Object parmaValue = parameter.getValue();
        if (StringUtils.equalsIgnoreCase(paramName, rowCountParamName)) {
          result = getRowCount(parmaValue);
        }
      }
    }
    return result;
  }

  private Integer getRowCount(Object rowCount) {
    return Integer.parseInt(String.valueOf(rowCount));
  }
}
