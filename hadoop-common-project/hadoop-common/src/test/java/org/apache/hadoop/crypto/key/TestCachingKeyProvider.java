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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCachingKeyProvider {

  @Test
  public void testCurrentKey() throws Exception {
    KeyProvider.KeyVersion mockKey = mock(KeyProvider.KeyVersion.class);
    KeyProvider mockProv = mock(KeyProvider.class);
    when(mockProv.getCurrentKey(eq("k1"))).thenReturn(mockKey);
    when(mockProv.getCurrentKey(eq("k2"))).thenReturn(null);
    when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);

    // asserting caching
    assertEquals(mockKey, cache.getCurrentKey("k1"));
    verify(mockProv, times(1)).getCurrentKey(eq("k1"));
    assertEquals(mockKey, cache.getCurrentKey("k1"));
    verify(mockProv, times(1)).getCurrentKey(eq("k1"));
    Thread.sleep(1200);
    assertEquals(mockKey, cache.getCurrentKey("k1"));
    verify(mockProv, times(2)).getCurrentKey(eq("k1"));

    // asserting no caching when key is not known
    cache = new CachingKeyProvider(mockProv, 100, 100);
    assertEquals(null, cache.getCurrentKey("k2"));
    verify(mockProv, times(1)).getCurrentKey(eq("k2"));
    assertEquals(null, cache.getCurrentKey("k2"));
    verify(mockProv, times(2)).getCurrentKey(eq("k2"));
  }

  @Test
  public void testKeyVersion() throws Exception {
    KeyProvider.KeyVersion mockKey = mock(KeyProvider.KeyVersion.class);
    KeyProvider mockProv = mock(KeyProvider.class);
    when(mockProv.getKeyVersion(eq("k1@0")))
        .thenReturn(mockKey);
    when(mockProv.getKeyVersion(eq("k2@0"))).thenReturn(null);
    when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);

    // asserting caching
    assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    verify(mockProv, times(1))
        .getKeyVersion(eq("k1@0"));
    assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    verify(mockProv, times(1))
        .getKeyVersion(eq("k1@0"));
    Thread.sleep(200);
    assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    verify(mockProv, times(2))
        .getKeyVersion(eq("k1@0"));

    // asserting no caching when key is not known
    cache = new CachingKeyProvider(mockProv, 100, 100);
    assertEquals(null, cache.getKeyVersion("k2@0"));
    verify(mockProv, times(1))
        .getKeyVersion(eq("k2@0"));
    assertEquals(null, cache.getKeyVersion("k2@0"));
    verify(mockProv, times(2))
        .getKeyVersion(eq("k2@0"));
  }

  @Test
  public void testMetadata() throws Exception {
    KeyProvider.Metadata mockMeta = mock(KeyProvider.Metadata.class);
    KeyProvider mockProv = mock(KeyProvider.class);
    when(mockProv.getMetadata(eq("k1"))).thenReturn(mockMeta);
    when(mockProv.getMetadata(eq("k2"))).thenReturn(null);
    when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);

    // asserting caching
    assertEquals(mockMeta, cache.getMetadata("k1"));
    verify(mockProv, times(1)).getMetadata(eq("k1"));
    assertEquals(mockMeta, cache.getMetadata("k1"));
    verify(mockProv, times(1)).getMetadata(eq("k1"));
    Thread.sleep(200);
    assertEquals(mockMeta, cache.getMetadata("k1"));
    verify(mockProv, times(2)).getMetadata(eq("k1"));

    // asserting no caching when key is not known
    cache = new CachingKeyProvider(mockProv, 100, 100);
    assertEquals(null, cache.getMetadata("k2"));
    verify(mockProv, times(1)).getMetadata(eq("k2"));
    assertEquals(null, cache.getMetadata("k2"));
    verify(mockProv, times(2)).getMetadata(eq("k2"));
  }

  @Test
  public void testRollNewVersion() throws Exception {
    KeyProvider.KeyVersion mockKey = mock(KeyProvider.KeyVersion.class);
    KeyProvider mockProv = mock(KeyProvider.class);
    when(mockProv.getCurrentKey(eq("k1"))).thenReturn(mockKey);
    when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);
    assertEquals(mockKey, cache.getCurrentKey("k1"));
    verify(mockProv, times(1)).getCurrentKey(eq("k1"));
    cache.rollNewVersion("k1");

    // asserting the cache is purged
    assertEquals(mockKey, cache.getCurrentKey("k1"));
    verify(mockProv, times(2)).getCurrentKey(eq("k1"));
    cache.rollNewVersion("k1", new byte[0]);
    assertEquals(mockKey, cache.getCurrentKey("k1"));
    verify(mockProv, times(3)).getCurrentKey(eq("k1"));
  }

  @Test
  public void testDeleteKey() throws Exception {
    KeyProvider.KeyVersion mockKey = mock(KeyProvider.KeyVersion.class);
    KeyProvider mockProv = mock(KeyProvider.class);
    when(mockProv.getCurrentKey(eq("k1"))).thenReturn(mockKey);
    when(mockProv.getKeyVersion(eq("k1@0")))
        .thenReturn(mockKey);
    when(mockProv.getMetadata(eq("k1"))).thenReturn(
        new KMSClientProvider.KMSMetadata("c", 0, "l", null, new Date(), 1));
    when(mockProv.getConf()).thenReturn(new Configuration());
    KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);
    assertEquals(mockKey, cache.getCurrentKey("k1"));
    verify(mockProv, times(1)).getCurrentKey(eq("k1"));
    assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    verify(mockProv, times(1))
        .getKeyVersion(eq("k1@0"));
    cache.deleteKey("k1");

    // asserting the cache is purged
    assertEquals(mockKey, cache.getCurrentKey("k1"));
    verify(mockProv, times(2)).getCurrentKey(eq("k1"));
    assertEquals(mockKey, cache.getKeyVersion("k1@0"));
    verify(mockProv, times(2))
        .getKeyVersion(eq("k1@0"));
  }
}
