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

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.security.SecureRandom;

public class TestKeyProviderCryptoExtension {

  private static final String CIPHER = "AES";

  @Test
  public void testGenerateEncryptedKey() throws Exception {
    Configuration conf = new Configuration();    
    KeyProvider kp = 
        new UserProvider.Factory().createProvider(new URI("user:///"), conf);
    KeyProvider.Options options = new KeyProvider.Options(conf);
    options.setCipher(CIPHER);
    options.setBitLength(128);
    KeyProvider.KeyVersion kv = kp.createKey("foo", SecureRandom.getSeed(16),
        options);
    KeyProviderCryptoExtension kpExt = 
        KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);
    
    KeyProviderCryptoExtension.EncryptedKeyVersion ek1 = 
        kpExt.generateEncryptedKey(kv);
    Assert.assertEquals(KeyProviderCryptoExtension.EEK, 
        ek1.getEncryptedKey().getVersionName());
    Assert.assertEquals("foo", ek1.getKeyName());
    Assert.assertNotNull(ek1.getEncryptedKey().getMaterial());
    Assert.assertEquals(kv.getMaterial().length, 
        ek1.getEncryptedKey().getMaterial().length);
    KeyProvider.KeyVersion k1 = kpExt.decryptEncryptedKey(ek1);
    Assert.assertEquals(KeyProviderCryptoExtension.EK, k1.getVersionName());
    KeyProvider.KeyVersion k1a = kpExt.decryptEncryptedKey(ek1);
    Assert.assertArrayEquals(k1.getMaterial(), k1a.getMaterial());
    Assert.assertEquals(kv.getMaterial().length, k1.getMaterial().length);

    KeyProviderCryptoExtension.EncryptedKeyVersion ek2 = 
        kpExt.generateEncryptedKey(kv);
    KeyProvider.KeyVersion k2 = kpExt.decryptEncryptedKey(ek2);
    boolean eq = true;
    for (int i = 0; eq && i < ek2.getEncryptedKey().getMaterial().length; i++) {
      eq = k2.getMaterial()[i] == k1.getMaterial()[i];
    }
    Assert.assertFalse(eq);
  }
}
