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
package org.apache.hadoop.crypto.key;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCachingKeyProvider {

  @Test
  public void testCurrentKey() throws Exception {
    KeyProvider.KeyVersion mockKey = Mockito.mock(KeyProvider.KeyVersion.class);
    KeyProvider mockProv = Mockito.mock(KeyProvider.class);
    Mockito.when(mockProv.getCurrentKey(Mockito.eq("k1"))).thenReturn(mockKey);
    Mockito.when(mockProv.getCurrentKey(Mockito.eq("k2"))).thenReturn(null);
    Mockito.when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);

    // asserting caching
    Assert.assertEquals(mockKey, cache.getCurrentKey("k1"));
    Mockito.verify(mockProv, Mockito.times(1)).getCurrentKey(Mockito.eq("k1"));
    Assert.assertEquals(mockKey, cache.getCurrentKey("k1"));
    Mockito.verify(mockProv, Mockito.times(1)).getCurrentKey(Mockito.eq("k1"));
    Thread.sleep(1200);
    Assert.assertEquals(mockKey, cache.getCurrentKey("k1"));
    Mockito.verify(mockProv, Mockito.times(2)).getCurrentKey(Mockito.eq("k1"));

    // asserting no caching when key is not known
    cache = new CachingKeyProvider(mockProv, 100, 100);
    Assert.assertEquals(null, cache.getCurrentKey("k2"));
    Mockito.verify(mockProv, Mockito.times(1)).getCurrentKey(Mockito.eq("k2"));
    Assert.assertEquals(null, cache.getCurrentKey("k2"));
    Mockito.verify(mockProv, Mockito.times(2)).getCurrentKey(Mockito.eq("k2"));
  }

  @Test
  public void testKeyVersion() throws Exception {
    KeyProvider.KeyVersion mockKey = Mockito.mock(KeyProvider.KeyVersion.class);
    KeyProvider mockProv = Mockito.mock(KeyProvider.class);
    Mockito.when(mockProv.getKeyVersion(Mockito.eq("k1@0")))
        .thenReturn(mockKey);
    Mockito.when(mockProv.getKeyVersion(Mockito.eq("k2@0"))).thenReturn(null);
    Mockito.when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);

    // asserting caching
    Assert.assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    Mockito.verify(mockProv, Mockito.times(1))
        .getKeyVersion(Mockito.eq("k1@0"));
    Assert.assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    Mockito.verify(mockProv, Mockito.times(1))
        .getKeyVersion(Mockito.eq("k1@0"));
    Thread.sleep(200);
    Assert.assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    Mockito.verify(mockProv, Mockito.times(2))
        .getKeyVersion(Mockito.eq("k1@0"));

    // asserting no caching when key is not known
    cache = new CachingKeyProvider(mockProv, 100, 100);
    Assert.assertEquals(null, cache.getKeyVersion("k2@0"));
    Mockito.verify(mockProv, Mockito.times(1))
        .getKeyVersion(Mockito.eq("k2@0"));
    Assert.assertEquals(null, cache.getKeyVersion("k2@0"));
    Mockito.verify(mockProv, Mockito.times(2))
        .getKeyVersion(Mockito.eq("k2@0"));
  }

  @Test
  public void testMetadata() throws Exception {
    KeyProvider.Metadata mockMeta = Mockito.mock(KeyProvider.Metadata.class);
    KeyProvider mockProv = Mockito.mock(KeyProvider.class);
    Mockito.when(mockProv.getMetadata(Mockito.eq("k1"))).thenReturn(mockMeta);
    Mockito.when(mockProv.getMetadata(Mockito.eq("k2"))).thenReturn(null);
    Mockito.when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);

    // asserting caching
    Assert.assertEquals(mockMeta, cache.getMetadata("k1"));
    Mockito.verify(mockProv, Mockito.times(1)).getMetadata(Mockito.eq("k1"));
    Assert.assertEquals(mockMeta, cache.getMetadata("k1"));
    Mockito.verify(mockProv, Mockito.times(1)).getMetadata(Mockito.eq("k1"));
    Thread.sleep(200);
    Assert.assertEquals(mockMeta, cache.getMetadata("k1"));
    Mockito.verify(mockProv, Mockito.times(2)).getMetadata(Mockito.eq("k1"));

    // asserting no caching when key is not known
    cache = new CachingKeyProvider(mockProv, 100, 100);
    Assert.assertEquals(null, cache.getMetadata("k2"));
    Mockito.verify(mockProv, Mockito.times(1)).getMetadata(Mockito.eq("k2"));
    Assert.assertEquals(null, cache.getMetadata("k2"));
    Mockito.verify(mockProv, Mockito.times(2)).getMetadata(Mockito.eq("k2"));
  }

  @Test
  public void testRollNewVersion() throws Exception {
    KeyProvider.KeyVersion mockKey = Mockito.mock(KeyProvider.KeyVersion.class);
    KeyProvider mockProv = Mockito.mock(KeyProvider.class);
    Mockito.when(mockProv.getCurrentKey(Mockito.eq("k1"))).thenReturn(mockKey);
    Mockito.when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);
    Assert.assertEquals(mockKey, cache.getCurrentKey("k1"));
    Mockito.verify(mockProv, Mockito.times(1)).getCurrentKey(Mockito.eq("k1"));
    cache.rollNewVersion("k1");

    // asserting the cache is purged
    Assert.assertEquals(mockKey, cache.getCurrentKey("k1"));
    Mockito.verify(mockProv, Mockito.times(2)).getCurrentKey(Mockito.eq("k1"));
    cache.rollNewVersion("k1", new byte[0]);
    Assert.assertEquals(mockKey, cache.getCurrentKey("k1"));
    Mockito.verify(mockProv, Mockito.times(3)).getCurrentKey(Mockito.eq("k1"));
  }

  @Test
  public void testDeleteKey() throws Exception {
    KeyProvider.KeyVersion mockKey = Mockito.mock(KeyProvider.KeyVersion.class);
    KeyProvider mockProv = Mockito.mock(KeyProvider.class);
    Mockito.when(mockProv.getCurrentKey(Mockito.eq("k1"))).thenReturn(mockKey);
    Mockito.when(mockProv.getKeyVersion(Mockito.eq("k1@0")))
        .thenReturn(mockKey);
    Mockito.when(mockProv.getMetadata(Mockito.eq("k1"))).thenReturn(
        new KMSClientProvider.KMSMetadata("c", 0, "l", null, new Date(), 1));
    Mockito.when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);
    Assert.assertEquals(mockKey, cache.getCurrentKey("k1"));
    Mockito.verify(mockProv, Mockito.times(1)).getCurrentKey(Mockito.eq("k1"));
    Assert.assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    Mockito.verify(mockProv, Mockito.times(1))
        .getKeyVersion(Mockito.eq("k1@0"));
    cache.deleteKey("k1");

    // asserting the cache is purged
    Assert.assertEquals(mockKey, cache.getCurrentKey("k1"));
    Mockito.verify(mockProv, Mockito.times(2)).getCurrentKey(Mockito.eq("k1"));
    Assert.assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    Mockito.verify(mockProv, Mockito.times(2))
        .getKeyVersion(Mockito.eq("k1@0"));
  }
}
