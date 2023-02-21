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

import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * RouterMasterKey Handler.
 * Used to parse the result information of the output parameter into the RouterMasterKey type.
 */
public class RouterMasterKeyHandler implements ResultSetHandler<RouterMasterKey> {

  private final static String MASTERKEY_OUT = "masterKey_OUT";

  @Override
  public RouterMasterKey handle(Object... params) throws SQLException {
    RouterMasterKey routerMasterKey = Records.newRecord(RouterMasterKey.class);
    for (Object param : params) {
      if (param instanceof FederationSQLOutParameter) {
        FederationSQLOutParameter parameter = (FederationSQLOutParameter) param;
        String paramName = parameter.getParamName();
        Object parmaValue = parameter.getValue();
        if (StringUtils.equalsIgnoreCase(paramName, MASTERKEY_OUT)) {
          DelegationKey key = getDelegationKey(parmaValue);
          routerMasterKey.setKeyId(key.getKeyId());
          routerMasterKey.setKeyBytes(ByteBuffer.wrap(key.getEncodedKey()));
          routerMasterKey.setExpiryDate(key.getExpiryDate());
        }
      }
    }
    return routerMasterKey;
  }

  private DelegationKey getDelegationKey(Object paramMasterKey) throws SQLException {
    try {
      DelegationKey key = new DelegationKey();
      String masterKey = String.valueOf(paramMasterKey);
      FederationStateStoreUtils.decodeWritable(key, masterKey);
      return key;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
}
