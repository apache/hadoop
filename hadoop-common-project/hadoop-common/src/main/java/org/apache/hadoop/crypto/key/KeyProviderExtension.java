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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * This is a utility class used to extend the functionality of KeyProvider, that
 * takes a KeyProvider and an Extension. It implements all the required methods
 * of the KeyProvider by delegating it to the provided KeyProvider.
 */
public abstract class KeyProviderExtension
<E extends KeyProviderExtension.Extension> extends KeyProvider {

  /**
   * A marker interface for the KeyProviderExtension subclass implement.
   */
  public static interface Extension {
  }

  private KeyProvider keyProvider;
  private E extension;

  public KeyProviderExtension(KeyProvider keyProvider, E extensions) {
    super(keyProvider.getConf());
    this.keyProvider = keyProvider;
    this.extension = extensions;
  }
  
  protected E getExtension() {
    return extension;
  }
  
  protected KeyProvider getKeyProvider() {
    return keyProvider;
  }

  @Override
  public boolean isTransient() {
    return keyProvider.isTransient();
  }

  @Override
  public Metadata[] getKeysMetadata(String... names) throws IOException {
    return keyProvider.getKeysMetadata(names);
  }

  @Override
  public KeyVersion getCurrentKey(String name) throws IOException {
    return keyProvider.getCurrentKey(name);
  }

  @Override
  public KeyVersion createKey(String name, Options options)
      throws NoSuchAlgorithmException, IOException {
    return keyProvider.createKey(name, options);
  }

  @Override
  public KeyVersion rollNewVersion(String name)
      throws NoSuchAlgorithmException, IOException {
    return keyProvider.rollNewVersion(name);
  }

  @Override
  public KeyVersion getKeyVersion(String versionName) throws IOException {
    return keyProvider.getKeyVersion(versionName);
  }

  @Override
  public List<String> getKeys() throws IOException {
    return keyProvider.getKeys();
  }

  @Override
  public List<KeyVersion> getKeyVersions(String name) throws IOException {
    return keyProvider.getKeyVersions(name);
  }

  @Override
  public Metadata getMetadata(String name) throws IOException {
    return keyProvider.getMetadata(name);
  }

  @Override
  public KeyVersion createKey(String name, byte[] material, Options options)
      throws IOException {
    return keyProvider.createKey(name, material, options);
  }

  @Override
  public void deleteKey(String name) throws IOException {
    keyProvider.deleteKey(name);
  }

  @Override
  public KeyVersion rollNewVersion(String name, byte[] material)
      throws IOException {
    return keyProvider.rollNewVersion(name, material);
  }

  @Override
  public void invalidateCache(String name) throws IOException {
    keyProvider.invalidateCache(name);
  }

  @Override
  public void flush() throws IOException {
    keyProvider.flush();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + keyProvider.toString();
  }
}
