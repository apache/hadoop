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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public class TestDFSClientCache {
  @Test
  public void testEviction() throws IOException {
    NfsConfiguration conf = new NfsConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost");

    // Only one entry will be in the cache
    final int MAX_CACHE_SIZE = 1;

    DFSClientCache cache = new DFSClientCache(conf, MAX_CACHE_SIZE);

    DFSClient c1 = cache.getDfsClient("test1");
    assertTrue(cache.getDfsClient("test1").toString().contains("ugi=test1"));
    assertEquals(c1, cache.getDfsClient("test1"));
    assertFalse(isDfsClientClose(c1));

    cache.getDfsClient("test2");
    assertTrue(isDfsClientClose(c1));
    assertTrue("cache size should be the max size or less",
        cache.clientCache.size() <= MAX_CACHE_SIZE);
  }

  @Test
  public void testGetUserGroupInformationSecure() throws IOException {
    String userName = "user1";
    String currentUser = "test-user";


    NfsConfiguration conf = new NfsConfiguration();
    UserGroupInformation currentUserUgi
            = UserGroupInformation.createRemoteUser(currentUser);
    currentUserUgi.setAuthenticationMethod(KERBEROS);
    UserGroupInformation.setLoginUser(currentUserUgi);

    DFSClientCache cache = new DFSClientCache(conf);
    UserGroupInformation ugiResult
            = cache.getUserGroupInformation(userName, currentUserUgi);

    assertThat(ugiResult.getUserName(), is(userName));
    assertThat(ugiResult.getRealUser(), is(currentUserUgi));
    assertThat(
            ugiResult.getAuthenticationMethod(),
            is(UserGroupInformation.AuthenticationMethod.PROXY));
  }

  @Test
  public void testGetUserGroupInformation() throws IOException {
    String userName = "user1";
    String currentUser = "currentUser";

    UserGroupInformation currentUserUgi = UserGroupInformation
            .createUserForTesting(currentUser, new String[0]);
    NfsConfiguration conf = new NfsConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost");
    DFSClientCache cache = new DFSClientCache(conf);
    UserGroupInformation ugiResult
            = cache.getUserGroupInformation(userName, currentUserUgi);

    assertThat(ugiResult.getUserName(), is(userName));
    assertThat(ugiResult.getRealUser(), is(currentUserUgi));
    assertThat(
            ugiResult.getAuthenticationMethod(),
            is(UserGroupInformation.AuthenticationMethod.PROXY));
  }

  private static boolean isDfsClientClose(DFSClient c) {
    try {
      c.exists("");
    } catch (IOException e) {
      return e.getMessage().equals("Filesystem closed");
    }
    return false;
  }
}
