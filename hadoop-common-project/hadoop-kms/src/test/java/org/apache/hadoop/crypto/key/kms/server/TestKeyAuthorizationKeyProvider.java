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
package org.apache.hadoop.crypto.key.kms.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.crypto.key.KeyProvider.Options;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.UserProvider;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyACLs;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;

public class TestKeyAuthorizationKeyProvider {

  private static final String CIPHER = "AES";

  @Test
  public void testCreateKey() throws Exception {
    final Configuration conf = new Configuration();
    KeyProvider kp = 
        new UserProvider.Factory().createProvider(new URI("user:///"), conf);
    KeyACLs mock = mock(KeyACLs.class);
    when(mock.isACLPresent("foo", KeyOpType.MANAGEMENT)).thenReturn(true);
    UserGroupInformation u1 = UserGroupInformation.createRemoteUser("u1");
    when(mock.hasAccessToKey("foo", u1, KeyOpType.MANAGEMENT)).thenReturn(true);
    final KeyProviderCryptoExtension kpExt =
        new KeyAuthorizationKeyProvider(
            KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp),
            mock);

    u1.doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              kpExt.createKey("foo", SecureRandom.getSeed(16),
                  newOptions(conf));
            } catch (IOException ioe) {
              Assert.fail("User should be Authorized !!");
            }

            // "bar" key not configured
            try {
              kpExt.createKey("bar", SecureRandom.getSeed(16),
                  newOptions(conf));
              Assert.fail("User should NOT be Authorized !!");
            } catch (IOException ioe) {
              // Ignore
            }
            return null;
          }
        }
        );

    // Unauthorized User
    UserGroupInformation.createRemoteUser("badGuy").doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            try {
              kpExt.createKey("foo", SecureRandom.getSeed(16),
                  newOptions(conf));
              Assert.fail("User should NOT be Authorized !!");
            } catch (IOException ioe) {
              // Ignore
            }
            return null;
          }
        }
        );
  }

  @Test
  public void testOpsWhenACLAttributeExists() throws Exception {
    final Configuration conf = new Configuration();
    KeyProvider kp = 
        new UserProvider.Factory().createProvider(new URI("user:///"), conf);
    KeyACLs mock = mock(KeyACLs.class);
    when(mock.isACLPresent("testKey", KeyOpType.MANAGEMENT)).thenReturn(true);
    when(mock.isACLPresent("testKey", KeyOpType.GENERATE_EEK)).thenReturn(true);
    when(mock.isACLPresent("testKey", KeyOpType.DECRYPT_EEK)).thenReturn(true);
    when(mock.isACLPresent("testKey", KeyOpType.ALL)).thenReturn(true);
    UserGroupInformation u1 = UserGroupInformation.createRemoteUser("u1");
    UserGroupInformation u2 = UserGroupInformation.createRemoteUser("u2");
    UserGroupInformation u3 = UserGroupInformation.createRemoteUser("u3");
    UserGroupInformation sudo = UserGroupInformation.createRemoteUser("sudo");
    when(mock.hasAccessToKey("testKey", u1, KeyOpType.MANAGEMENT)).thenReturn(true);
    when(mock.hasAccessToKey("testKey", u2, KeyOpType.GENERATE_EEK)).thenReturn(true);
    when(mock.hasAccessToKey("testKey", u3, KeyOpType.DECRYPT_EEK)).thenReturn(true);
    when(mock.hasAccessToKey("testKey", sudo, KeyOpType.ALL)).thenReturn(true);
    final KeyProviderCryptoExtension kpExt =
        new KeyAuthorizationKeyProvider(
            KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp),
            mock);

    final KeyVersion barKv = u1.doAs(
        new PrivilegedExceptionAction<KeyVersion>() {
          @Override
          public KeyVersion run() throws Exception {
            Options opt = newOptions(conf);
            Map<String, String> m = new HashMap<String, String>();
            m.put("key.acl.name", "testKey");
            opt.setAttributes(m);
            try {
              KeyVersion kv = 
                  kpExt.createKey("foo", SecureRandom.getSeed(16), opt);
              kpExt.rollNewVersion(kv.getName());
              kpExt.rollNewVersion(kv.getName(), SecureRandom.getSeed(16));
              kpExt.deleteKey(kv.getName());
            } catch (IOException ioe) {
              Assert.fail("User should be Authorized !!");
            }

            KeyVersion retkv = null;
            try {
              retkv = kpExt.createKey("bar", SecureRandom.getSeed(16), opt);
              kpExt.generateEncryptedKey(retkv.getName());
              Assert.fail("User should NOT be Authorized to generate EEK !!");
            } catch (IOException ioe) {
            }
            Assert.assertNotNull(retkv);
            return retkv;
          }
        }
        );

    final EncryptedKeyVersion barEKv =
        u2.doAs(
            new PrivilegedExceptionAction<EncryptedKeyVersion>() {
              @Override
              public EncryptedKeyVersion run() throws Exception {
                try {
                  kpExt.deleteKey(barKv.getName());
                  Assert.fail("User should NOT be Authorized to "
                      + "perform any other operation !!");
                } catch (IOException ioe) {
                }
                return kpExt.generateEncryptedKey(barKv.getName());
              }
            });

    u3.doAs(
        new PrivilegedExceptionAction<KeyVersion>() {
          @Override
          public KeyVersion run() throws Exception {
            try {
              kpExt.deleteKey(barKv.getName());
              Assert.fail("User should NOT be Authorized to "
                  + "perform any other operation !!");
            } catch (IOException ioe) {
            }
            return kpExt.decryptEncryptedKey(barEKv);
          }
        });

    sudo.doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            Options opt = newOptions(conf);
            Map<String, String> m = new HashMap<String, String>();
            m.put("key.acl.name", "testKey");
            opt.setAttributes(m);
            try {
              KeyVersion kv = 
                  kpExt.createKey("foo", SecureRandom.getSeed(16), opt);
              kpExt.rollNewVersion(kv.getName());
              kpExt.rollNewVersion(kv.getName(), SecureRandom.getSeed(16));
              EncryptedKeyVersion ekv = kpExt.generateEncryptedKey(kv.getName());
              kpExt.decryptEncryptedKey(ekv);
              kpExt.deleteKey(kv.getName());
            } catch (IOException ioe) {
              Assert.fail("User should be Allowed to do everything !!");
            }
            return null;
          }
        }
        );
  }

  private static KeyProvider.Options newOptions(Configuration conf) {
    KeyProvider.Options options = new KeyProvider.Options(conf);
    options.setCipher(CIPHER);
    options.setBitLength(128);
    return options;
  }


  @Test(expected = IllegalArgumentException.class)
  public void testDecryptWithKeyVersionNameKeyMismatch() throws Exception {
    final Configuration conf = new Configuration();
    KeyProvider kp =
        new UserProvider.Factory().createProvider(new URI("user:///"), conf);
    KeyACLs mock = mock(KeyACLs.class);
    when(mock.isACLPresent("testKey", KeyOpType.MANAGEMENT)).thenReturn(true);
    when(mock.isACLPresent("testKey", KeyOpType.GENERATE_EEK)).thenReturn(true);
    when(mock.isACLPresent("testKey", KeyOpType.DECRYPT_EEK)).thenReturn(true);
    when(mock.isACLPresent("testKey", KeyOpType.ALL)).thenReturn(true);
    UserGroupInformation u1 = UserGroupInformation.createRemoteUser("u1");
    UserGroupInformation u2 = UserGroupInformation.createRemoteUser("u2");
    UserGroupInformation u3 = UserGroupInformation.createRemoteUser("u3");
    UserGroupInformation sudo = UserGroupInformation.createRemoteUser("sudo");
    when(mock.hasAccessToKey("testKey", u1,
        KeyOpType.MANAGEMENT)).thenReturn(true);
    when(mock.hasAccessToKey("testKey", u2,
        KeyOpType.GENERATE_EEK)).thenReturn(true);
    when(mock.hasAccessToKey("testKey", u3,
        KeyOpType.DECRYPT_EEK)).thenReturn(true);
    when(mock.hasAccessToKey("testKey", sudo,
        KeyOpType.ALL)).thenReturn(true);
    final KeyProviderCryptoExtension kpExt =
        new KeyAuthorizationKeyProvider(
            KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp),
            mock);

    sudo.doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            Options opt = newOptions(conf);
            Map<String, String> m = new HashMap<String, String>();
            m.put("key.acl.name", "testKey");
            opt.setAttributes(m);
            KeyVersion kv =
                kpExt.createKey("foo", SecureRandom.getSeed(16), opt);
            kpExt.rollNewVersion(kv.getName());
            kpExt.rollNewVersion(kv.getName(), SecureRandom.getSeed(16));
            EncryptedKeyVersion ekv = kpExt.generateEncryptedKey(kv.getName());
            ekv = EncryptedKeyVersion.createForDecryption(
                ekv.getEncryptionKeyName() + "x",
                ekv.getEncryptionKeyVersionName(),
                ekv.getEncryptedKeyIv(),
                ekv.getEncryptedKeyVersion().getMaterial());
            kpExt.decryptEncryptedKey(ekv);
            return null;
          }
        }
    );
  }

}
