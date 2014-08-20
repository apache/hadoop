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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

public class TestDelegationTokenManager {

  private static final long DAY_IN_SECS = 86400;

  @Test
  public void testDTManager() throws Exception {
    DelegationTokenManager tm = new DelegationTokenManager(new Text("foo"),
        DAY_IN_SECS, DAY_IN_SECS, DAY_IN_SECS, DAY_IN_SECS);
    tm.init();
    Token<DelegationTokenIdentifier> token =
        tm.createToken(UserGroupInformation.getCurrentUser(), "foo");
    Assert.assertNotNull(token);
    tm.verifyToken(token);
    Assert.assertTrue(tm.renewToken(token, "foo") > System.currentTimeMillis());
    tm.cancelToken(token, "foo");
    try {
      tm.verifyToken(token);
      Assert.fail();
    } catch (IOException ex) {
      //NOP
    } catch (Exception ex) {
      Assert.fail();
    }
    tm.destroy();
  }

}
