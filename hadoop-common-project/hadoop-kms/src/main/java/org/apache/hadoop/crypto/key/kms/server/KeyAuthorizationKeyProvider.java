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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * A {@link KeyProvider} proxy that checks whether the current user derived via
 * {@link UserGroupInformation}, is authorized to perform the following
 * type of operations on a Key :
 * <ol>
 * <li>MANAGEMENT operations : createKey, rollNewVersion, deleteKey</li>
 * <li>GENERATE_EEK operations : generateEncryptedKey, warmUpEncryptedKeys</li>
 * <li>DECRYPT_EEK operation : decryptEncryptedKey</li>
 * <li>READ operations : getKeyVersion, getKeyVersions, getMetadata,
 * getKeysMetadata, getCurrentKey</li>
 * </ol>
 * The read operations (getCurrentKeyVersion / getMetadata) etc are not checked.
 */
public class KeyAuthorizationKeyProvider extends KeyProviderCryptoExtension {

  public static final String KEY_ACL = "key.acl.";
  private static final String KEY_ACL_NAME = KEY_ACL + "name";

  public enum KeyOpType {
    ALL, READ, MANAGEMENT, GENERATE_EEK, DECRYPT_EEK;
  }

  /**
   * Interface that needs to be implemented by a client of the
   * <code>KeyAuthorizationKeyProvider</code>.
   */
  public static interface KeyACLs {
    
    /**
     * This is called by the KeyProvider to check if the given user is
     * authorized to perform the specified operation on the given acl name.
     * @param aclName name of the key ACL
     * @param ugi User's UserGroupInformation
     * @param opType Operation Type 
     * @return true if user has access to the aclName and opType else false
     */
    public boolean hasAccessToKey(String aclName, UserGroupInformation ugi,
        KeyOpType opType);

    /**
     * 
     * @param aclName ACL name
     * @param opType Operation Type
     * @return true if AclName exists else false 
     */
    public boolean isACLPresent(String aclName, KeyOpType opType);
  }

  private final KeyProviderCryptoExtension provider;
  private final KeyACLs acls;
  private Lock readLock;
  private Lock writeLock;

  /**
   * The constructor takes a {@link KeyProviderCryptoExtension} and an
   * implementation of <code>KeyACLs</code>. All calls are delegated to the
   * provider keyProvider after authorization check (if required)
   * @param keyProvider  the key provider
   * @param acls the Key ACLs
   */
  public KeyAuthorizationKeyProvider(KeyProviderCryptoExtension keyProvider,
      KeyACLs acls) {
    super(keyProvider, null);
    this.provider = keyProvider;
    this.acls = acls;
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  // This method first checks if "key.acl.name" attribute is present as an
  // attribute in the provider Options. If yes, use the aclName for any
  // subsequent access checks, else use the keyName as the aclName and set it
  // as the value of the "key.acl.name" in the key's metadata.
  private void authorizeCreateKey(String keyName, Options options,
      UserGroupInformation ugi) throws IOException{
    Preconditions.checkNotNull(ugi, "UserGroupInformation cannot be null");
    Map<String, String> attributes = options.getAttributes();
    String aclName = attributes.get(KEY_ACL_NAME);
    boolean success = false;
    if (Strings.isNullOrEmpty(aclName)) {
      if (acls.isACLPresent(keyName, KeyOpType.MANAGEMENT)) {
        options.setAttributes(ImmutableMap.<String, String> builder()
            .putAll(attributes).put(KEY_ACL_NAME, keyName).build());
        success =
            acls.hasAccessToKey(keyName, ugi, KeyOpType.MANAGEMENT)
                || acls.hasAccessToKey(keyName, ugi, KeyOpType.ALL);
      } else {
        success = false;
      }
    } else {
      success = acls.isACLPresent(aclName, KeyOpType.MANAGEMENT) &&
          (acls.hasAccessToKey(aclName, ugi, KeyOpType.MANAGEMENT)
          || acls.hasAccessToKey(aclName, ugi, KeyOpType.ALL));
    }
    if (!success)
      throw new AuthorizationException(String.format("User [%s] is not"
          + " authorized to create key !!", ugi.getShortUserName()));
  }

  private void checkAccess(String aclName, UserGroupInformation ugi,
      KeyOpType opType) throws AuthorizationException {
    Preconditions.checkNotNull(aclName, "Key ACL name cannot be null");
    Preconditions.checkNotNull(ugi, "UserGroupInformation cannot be null");
    if (acls.isACLPresent(aclName, opType) &&
        (acls.hasAccessToKey(aclName, ugi, opType)
            || acls.hasAccessToKey(aclName, ugi, KeyOpType.ALL))) {
      return;
    } else {
      throw new AuthorizationException(String.format("User [%s] is not"
          + " authorized to perform [%s] on key with ACL name [%s]!!",
          ugi.getShortUserName(), opType, aclName));
    }
  }

  @Override
  public KeyVersion createKey(String name, Options options)
      throws NoSuchAlgorithmException, IOException {
    writeLock.lock();
    try {
      authorizeCreateKey(name, options, getUser());
      return provider.createKey(name, options);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public KeyVersion createKey(String name, byte[] material, Options options)
      throws IOException {
    writeLock.lock();
    try {
      authorizeCreateKey(name, options, getUser());
      return provider.createKey(name, material, options);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public KeyVersion rollNewVersion(String name)
      throws NoSuchAlgorithmException, IOException {
    writeLock.lock();
    try {
      doAccessCheck(name, KeyOpType.MANAGEMENT);
      return provider.rollNewVersion(name);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteKey(String name) throws IOException {
    writeLock.lock();
    try {
      doAccessCheck(name, KeyOpType.MANAGEMENT);
      provider.deleteKey(name);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public KeyVersion rollNewVersion(String name, byte[] material)
      throws IOException {
    writeLock.lock();
    try {
      doAccessCheck(name, KeyOpType.MANAGEMENT);
      return provider.rollNewVersion(name, material);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void invalidateCache(String name) throws IOException {
    writeLock.lock();
    try {
      doAccessCheck(name, KeyOpType.MANAGEMENT);
      provider.invalidateCache(name);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void warmUpEncryptedKeys(String... names) throws IOException {
    readLock.lock();
    try {
      for (String name : names) {
        doAccessCheck(name, KeyOpType.GENERATE_EEK);
      }
      provider.warmUpEncryptedKeys(names);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public EncryptedKeyVersion generateEncryptedKey(String encryptionKeyName)
      throws IOException, GeneralSecurityException {
    readLock.lock();
    try {
      doAccessCheck(encryptionKeyName, KeyOpType.GENERATE_EEK);
      return provider.generateEncryptedKey(encryptionKeyName);
    } finally {
      readLock.unlock();
    }
  }

  private void verifyKeyVersionBelongsToKey(EncryptedKeyVersion ekv)
      throws IOException {
    String kn = ekv.getEncryptionKeyName();
    String kvn = ekv.getEncryptionKeyVersionName();
    KeyVersion kv = provider.getKeyVersion(kvn);
    if (kv == null) {
      throw new IllegalArgumentException(String.format(
          "'%s' not found", kvn));
    }
    if (!kv.getName().equals(kn)) {
      throw new IllegalArgumentException(String.format(
          "KeyVersion '%s' does not belong to the key '%s'", kvn, kn));
    }
  }

  @Override
  public KeyVersion decryptEncryptedKey(EncryptedKeyVersion encryptedKeyVersion)
          throws IOException, GeneralSecurityException {
    readLock.lock();
    try {
      verifyKeyVersionBelongsToKey(encryptedKeyVersion);
      doAccessCheck(
          encryptedKeyVersion.getEncryptionKeyName(), KeyOpType.DECRYPT_EEK);
      return provider.decryptEncryptedKey(encryptedKeyVersion);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public EncryptedKeyVersion reencryptEncryptedKey(EncryptedKeyVersion ekv)
      throws IOException, GeneralSecurityException {
    readLock.lock();
    try {
      verifyKeyVersionBelongsToKey(ekv);
      doAccessCheck(ekv.getEncryptionKeyName(), KeyOpType.GENERATE_EEK);
      return provider.reencryptEncryptedKey(ekv);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void reencryptEncryptedKeys(List<EncryptedKeyVersion> ekvs)
      throws IOException, GeneralSecurityException {
    if (ekvs.isEmpty()) {
      return;
    }
    readLock.lock();
    try {
      for (EncryptedKeyVersion ekv : ekvs) {
        verifyKeyVersionBelongsToKey(ekv);
      }
      final String keyName = ekvs.get(0).getEncryptionKeyName();
      doAccessCheck(keyName, KeyOpType.GENERATE_EEK);
      provider.reencryptEncryptedKeys(ekvs);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public KeyVersion getKeyVersion(String versionName) throws IOException {
    readLock.lock();
    try {
      KeyVersion keyVersion = provider.getKeyVersion(versionName);
      if (keyVersion != null) {
        doAccessCheck(keyVersion.getName(), KeyOpType.READ);
      }
      return keyVersion;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getKeys() throws IOException {
    return provider.getKeys();
  }

  @Override
  public List<KeyVersion> getKeyVersions(String name) throws IOException {
    readLock.lock();
    try {
      doAccessCheck(name, KeyOpType.READ);
      return provider.getKeyVersions(name);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Metadata getMetadata(String name) throws IOException {
    readLock.lock();
    try {
      doAccessCheck(name, KeyOpType.READ);
      return provider.getMetadata(name);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Metadata[] getKeysMetadata(String... names) throws IOException {
    readLock.lock();
    try {
      for (String name : names) {
        doAccessCheck(name, KeyOpType.READ);
      }
      return provider.getKeysMetadata(names);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public KeyVersion getCurrentKey(String name) throws IOException {
    readLock.lock();
    try {
      doAccessCheck(name, KeyOpType.READ);
      return provider.getCurrentKey(name);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void flush() throws IOException {
    provider.flush();
  }

  @Override
  public boolean isTransient() {
    return provider.isTransient();
  }

  private void doAccessCheck(String keyName, KeyOpType opType) throws
      IOException {
    Metadata metadata = provider.getMetadata(keyName);
    if (metadata != null) {
      String aclName = metadata.getAttributes().get(KEY_ACL_NAME);
      checkAccess((aclName == null) ? keyName : aclName, getUser(), opType);
    }
  }

  private UserGroupInformation getUser() throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  @Override
  protected KeyProvider getKeyProvider() {
    return this;
  }

  @Override
  public String toString() {
    return this.getClass().getName()+": "+provider.toString();
  }

}
