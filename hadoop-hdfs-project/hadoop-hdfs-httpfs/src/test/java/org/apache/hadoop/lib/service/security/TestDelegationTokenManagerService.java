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

package org.apache.hadoop.lib.service.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.http.server.HttpFSServerWebApp;
import org.apache.hadoop.lib.server.Server;
import org.apache.hadoop.lib.service.DelegationTokenManager;
import org.apache.hadoop.lib.service.DelegationTokenManagerException;
import org.apache.hadoop.lib.service.hadoop.FileSystemAccessService;
import org.apache.hadoop.lib.service.instrumentation.InstrumentationService;
import org.apache.hadoop.lib.service.scheduler.SchedulerService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.HTestCase;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

public class TestDelegationTokenManagerService extends HTestCase {

  @Test
  @TestDir
  public void service() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("httpfs.services", StringUtils.join(",",
      Arrays.asList(InstrumentationService.class.getName(),
          SchedulerService.class.getName(),
          FileSystemAccessService.class.getName(),
          DelegationTokenManagerService.class.getName())));
    Server server = new HttpFSServerWebApp(dir, dir, dir, dir, conf);
    server.init();
    DelegationTokenManager tm = server.get(DelegationTokenManager.class);
    Assert.assertNotNull(tm);
    server.destroy();
  }

  @Test
  @TestDir
  @SuppressWarnings("unchecked")
  public void tokens() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",",
      Arrays.asList(DelegationTokenManagerService.class.getName())));
    HttpFSServerWebApp server = new HttpFSServerWebApp(dir, dir, dir, dir, conf);
    server.setAuthority(new InetSocketAddress(InetAddress.getLocalHost(), 14000));
    server.init();
    DelegationTokenManager tm = server.get(DelegationTokenManager.class);
    Token token = tm.createToken(UserGroupInformation.getCurrentUser(), "foo");
    Assert.assertNotNull(token);
    tm.verifyToken(token);
    Assert.assertTrue(tm.renewToken(token, "foo") > System.currentTimeMillis());
    tm.cancelToken(token, "foo");
    try {
      tm.verifyToken(token);
      Assert.fail();
    } catch (DelegationTokenManagerException ex) {
      //NOP
    } catch (Exception ex) {
      Assert.fail();
    }
    server.destroy();
  }

}
