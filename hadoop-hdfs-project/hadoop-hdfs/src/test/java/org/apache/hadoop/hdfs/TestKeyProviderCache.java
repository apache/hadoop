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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.junit.Assert;
import org.junit.Test;

public class TestKeyProviderCache {

  public static class DummyKeyProvider extends KeyProvider {

    public DummyKeyProvider(Configuration conf) {
      super(conf);
    }

    @Override
    public KeyVersion getKeyVersion(String versionName) throws IOException {
      return null;
    }

    @Override
    public List<String> getKeys() throws IOException {
      return null;
    }

    @Override
    public List<KeyVersion> getKeyVersions(String name) throws IOException {
      return null;
    }

    @Override
    public Metadata getMetadata(String name) throws IOException {
      return null;
    }

    @Override
    public KeyVersion createKey(String name, byte[] material, Options options)
        throws IOException {
      return null;
    }

    @Override
    public void deleteKey(String name) throws IOException {
    }

    @Override
    public KeyVersion rollNewVersion(String name, byte[] material)
        throws IOException {
      return null;
    }

    @Override
    public void flush() throws IOException {
    }

  }

  public static class Factory extends KeyProviderFactory {

    @Override
    public KeyProvider createProvider(URI providerName, Configuration conf)
        throws IOException {
      if ("dummy".equals(providerName.getScheme())) {
        return new DummyKeyProvider(conf);
      }
      return null;
    }
  }

  @Test
  public void testCache() throws Exception {
    KeyProviderCache kpCache = new KeyProviderCache(10000);
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI,
        "dummy://foo:bar@test_provider1");
    KeyProvider keyProvider1 = kpCache.get(conf);
    Assert.assertNotNull("Returned Key Provider is null !!", keyProvider1);

    conf.set(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI,
        "dummy://foo:bar@test_provider1");
    KeyProvider keyProvider2 = kpCache.get(conf);

    Assert.assertTrue("Different KeyProviders returned !!",
        keyProvider1 == keyProvider2);

    conf.set(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI,
        "dummy://test_provider3");
    KeyProvider keyProvider3 = kpCache.get(conf);

    Assert.assertFalse("Same KeyProviders returned !!",
        keyProvider1 == keyProvider3);

    conf.set(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI,
        "dummy://hello:there@test_provider1");
    KeyProvider keyProvider4 = kpCache.get(conf);

    Assert.assertFalse("Same KeyProviders returned !!",
        keyProvider1 == keyProvider4);

  }
}
