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
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.sql.SQLException;

import static org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils.decodeWritable;

/**
 * RouterStoreToken Handler.
 * Used to parse the result information of the output parameter into the RouterStoreToken type.
 */
public class RouterStoreTokenHandler implements ResultSetHandler<RouterStoreToken> {

  private final static String TOKENIDENT_OUT = "tokenIdent_OUT";
  private final static String TOKEN_OUT = "token_OUT";
  private final static String RENEWDATE_OUT = "renewDate_OUT";

  @Override
  public RouterStoreToken handle(Object... params) throws SQLException {
    RouterStoreToken storeToken = Records.newRecord(RouterStoreToken.class);
    for (Object param : params) {
      if (param instanceof FederationSQLOutParameter) {
        FederationSQLOutParameter parameter = (FederationSQLOutParameter) param;
        String paramName = parameter.getParamName();
        Object parmaValue = parameter.getValue();
        if (StringUtils.equalsIgnoreCase(paramName, TOKENIDENT_OUT)) {
          YARNDelegationTokenIdentifier identifier = getYARNDelegationTokenIdentifier(parmaValue);
          storeToken.setIdentifier(identifier);
        } else if (StringUtils.equalsIgnoreCase(paramName, TOKEN_OUT)) {
          String tokenInfo = getTokenInfo(parmaValue);
          storeToken.setTokenInfo(tokenInfo);
        } else if(StringUtils.equalsIgnoreCase(paramName, RENEWDATE_OUT)){
          Long renewDate = getRenewDate(parmaValue);
          storeToken.setRenewDate(renewDate);
        }
      }
    }
    return storeToken;
  }

  private YARNDelegationTokenIdentifier getYARNDelegationTokenIdentifier(Object tokenIdent)
      throws SQLException {
    try {
      YARNDelegationTokenIdentifier resultIdentifier =
          Records.newRecord(YARNDelegationTokenIdentifier.class);
      decodeWritable(resultIdentifier, String.valueOf(tokenIdent));
      return resultIdentifier;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  private String getTokenInfo(Object tokenInfo) {
    return String.valueOf(tokenInfo);
  }

  private Long getRenewDate(Object renewDate) {
    return Long.parseLong(String.valueOf(renewDate));
  }
}
