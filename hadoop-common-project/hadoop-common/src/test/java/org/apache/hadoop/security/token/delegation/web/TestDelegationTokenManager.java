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
package org.apache.hadoop.security.token.delegation.web;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestDelegationTokenManager {

  private static final long DAY_IN_SECS = 86400;

  public static Collection<Object[]> headers() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  private boolean enableZKKey;

  public void initTestDelegationTokenManager(boolean pEnableZKKey) {
    this.enableZKKey = pEnableZKKey;
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("headers")
  public void testDTManager(boolean pEnableZKKey) throws Exception {
    initTestDelegationTokenManager(pEnableZKKey);
    Configuration conf = new Configuration(false);
    conf.setLong(DelegationTokenManager.UPDATE_INTERVAL, DAY_IN_SECS);
    conf.setLong(DelegationTokenManager.MAX_LIFETIME, DAY_IN_SECS);
    conf.setLong(DelegationTokenManager.RENEW_INTERVAL, DAY_IN_SECS);
    conf.setLong(DelegationTokenManager.REMOVAL_SCAN_INTERVAL, DAY_IN_SECS);
    conf.getBoolean(DelegationTokenManager.ENABLE_ZK_KEY, enableZKKey);
    DelegationTokenManager tm =
        new DelegationTokenManager(conf, new Text("foo"));
    tm.init();
    Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>) tm.createToken(
            UserGroupInformation.getCurrentUser(), "foo");
    Assertions.assertNotNull(token);
    tm.verifyToken(token);
    Assertions.assertTrue(tm.renewToken(token, "foo") > System.currentTimeMillis());
    tm.cancelToken(token, "foo");
    try {
      tm.verifyToken(token);
      Assertions.fail();
    } catch (IOException ex) {
      //NOP
    } catch (Exception ex) {
      Assertions.fail();
    }
    tm.destroy();
  }

}
