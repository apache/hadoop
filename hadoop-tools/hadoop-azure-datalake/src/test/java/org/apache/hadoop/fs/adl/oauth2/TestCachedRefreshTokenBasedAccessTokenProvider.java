/*
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
 *
 */
package org.apache.hadoop.fs.adl.oauth2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.oauth2.AccessTokenProvider;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_CLIENT_ID_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY;
import static org.apache.hadoop.hdfs.web.oauth2.ConfRefreshTokenBasedAccessTokenProvider.OAUTH_REFRESH_TOKEN_KEY;

/**
 * Verify cache behavior of ConfRefreshTokenBasedAccessTokenProvider instances.
 */
public class TestCachedRefreshTokenBasedAccessTokenProvider {

  private Configuration conf;

  @Rule public TestName name = new TestName();
  String clientId(int id) {
    return name.getMethodName() + "_clientID" + id;
  }

  @Before
  public void initConfig() {
    conf = new Configuration(false);
    conf.set(OAUTH_CLIENT_ID_KEY, clientId(0));
    conf.set(OAUTH_REFRESH_TOKEN_KEY, "01234567890abcdef");
    conf.set(OAUTH_REFRESH_URL_KEY, "http://dingo.invalid:80");
  }

  @Test
  public void testCacheInstance() throws Exception {
    final AccessTokenProvider inst0 = mock(AccessTokenProvider.class);
    when(inst0.getConf()).thenReturn(conf);

    // verify config
    CachedRefreshTokenBasedAccessTokenProvider t1 = new MockProvider(inst0);
    t1.setConf(conf);
    verify(inst0).setConf(any(Configuration.class)); // cloned, not exact match

    // verify cache hit
    CachedRefreshTokenBasedAccessTokenProvider t2 =
        new CachedRefreshTokenBasedAccessTokenProvider() {
          @Override
          AccessTokenProvider newInstance() {
            fail("Failed to return cached instance");
            return null;
          }
        };
    t2.setConf(conf);

    // verify force refresh
    conf.setBoolean(
        CachedRefreshTokenBasedAccessTokenProvider.FORCE_REFRESH, true);
    final AccessTokenProvider inst1 = mock(AccessTokenProvider.class);
    when(inst1.getConf()).thenReturn(conf);
    CachedRefreshTokenBasedAccessTokenProvider t3 = new MockProvider(inst1);
    t3.setConf(conf);
    verify(inst1).setConf(any(Configuration.class));

    // verify cache miss
    conf.set(OAUTH_REFRESH_URL_KEY, "http://yak.invalid:80");
    final AccessTokenProvider inst2 = mock(AccessTokenProvider.class);
    when(inst2.getConf()).thenReturn(conf);
    CachedRefreshTokenBasedAccessTokenProvider t4 = new MockProvider(inst2);
    t4.setConf(conf);
    verify(inst2).setConf(any(Configuration.class));
  }

  @Test
  public void testCacheLimit() throws Exception {
    final int iter = CachedRefreshTokenBasedAccessTokenProvider.MAX_PROVIDERS;
    for (int i = 0; i < iter; ++i) {
      conf.set(OAUTH_CLIENT_ID_KEY, clientId(i));
      AccessTokenProvider inst = mock(AccessTokenProvider.class);
      when(inst.getConf()).thenReturn(conf);
      CachedRefreshTokenBasedAccessTokenProvider t = new MockProvider(inst);
      t.setConf(conf);
      verify(inst).setConf(any(Configuration.class));
    }
    // verify cache hit
    for (int i = 0; i < iter; ++i) {
      conf.set(OAUTH_CLIENT_ID_KEY, clientId(i));
      CachedRefreshTokenBasedAccessTokenProvider t =
          new CachedRefreshTokenBasedAccessTokenProvider() {
            @Override
            AccessTokenProvider newInstance() {
              fail("Failed to return cached instance");
              return null;
            }
          };
      t.setConf(conf);
    }

    // verify miss, evict 0
    conf.set(OAUTH_CLIENT_ID_KEY, clientId(iter));
    final AccessTokenProvider inst = mock(AccessTokenProvider.class);
    when(inst.getConf()).thenReturn(conf);
    CachedRefreshTokenBasedAccessTokenProvider t = new MockProvider(inst);
    t.setConf(conf);
    verify(inst).setConf(any(Configuration.class));

    // verify miss
    conf.set(OAUTH_CLIENT_ID_KEY, clientId(0));
    final AccessTokenProvider inst0 = mock(AccessTokenProvider.class);
    when(inst0.getConf()).thenReturn(conf);
    CachedRefreshTokenBasedAccessTokenProvider t0 = new MockProvider(inst0);
    t0.setConf(conf);
    verify(inst0).setConf(any(Configuration.class));
  }

  static class MockProvider extends CachedRefreshTokenBasedAccessTokenProvider {
    private final AccessTokenProvider inst;
    MockProvider(AccessTokenProvider inst) {
      this.inst = inst;
    }
    @Override
    AccessTokenProvider newInstance() {
      return inst;
    }
  }

}
