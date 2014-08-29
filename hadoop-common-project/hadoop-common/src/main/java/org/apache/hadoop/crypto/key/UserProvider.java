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
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A KeyProvider factory for UGIs. It uses the credentials object associated
 * with the current user to find keys. This provider is created using a
 * URI of "user:///".
 */
@InterfaceAudience.Private
public class UserProvider extends KeyProvider {
  public static final String SCHEME_NAME = "user";
  private final UserGroupInformation user;
  private final Credentials credentials;
  private final Map<String, Metadata> cache = new HashMap<String, Metadata>();

  private UserProvider(Configuration conf) throws IOException {
    super(conf);
    user = UserGroupInformation.getCurrentUser();
    credentials = user.getCredentials();
  }

  @Override
  public boolean isTransient() {
    return true;
  }

  @Override
  public synchronized KeyVersion getKeyVersion(String versionName)
      throws IOException {
    byte[] bytes = credentials.getSecretKey(new Text(versionName));
    if (bytes == null) {
      return null;
    }
    return new KeyVersion(getBaseName(versionName), versionName, bytes);
  }

  @Override
  public synchronized Metadata getMetadata(String name) throws IOException {
    if (cache.containsKey(name)) {
      return cache.get(name);
    }
    byte[] serialized = credentials.getSecretKey(new Text(name));
    if (serialized == null) {
      return null;
    }
    Metadata result = new Metadata(serialized);
    cache.put(name, result);
    return result;
  }

  @Override
  public synchronized KeyVersion createKey(String name, byte[] material,
                               Options options) throws IOException {
    Text nameT = new Text(name);
    if (credentials.getSecretKey(nameT) != null) {
      throw new IOException("Key " + name + " already exists in " + this);
    }
    if (options.getBitLength() != 8 * material.length) {
      throw new IOException("Wrong key length. Required " +
          options.getBitLength() + ", but got " + (8 * material.length));
    }
    Metadata meta = new Metadata(options.getCipher(), options.getBitLength(),
        options.getDescription(), options.getAttributes(), new Date(), 1);
    cache.put(name, meta);
    String versionName = buildVersionName(name, 0);
    credentials.addSecretKey(nameT, meta.serialize());
    credentials.addSecretKey(new Text(versionName), material);
    return new KeyVersion(name, versionName, material);
  }

  @Override
  public synchronized void deleteKey(String name) throws IOException {
    Metadata meta = getMetadata(name);
    if (meta == null) {
      throw new IOException("Key " + name + " does not exist in " + this);
    }
    for(int v=0; v < meta.getVersions(); ++v) {
      credentials.removeSecretKey(new Text(buildVersionName(name, v)));
    }
    credentials.removeSecretKey(new Text(name));
    cache.remove(name);
  }

  @Override
  public synchronized KeyVersion rollNewVersion(String name,
                                    byte[] material) throws IOException {
    Metadata meta = getMetadata(name);
    if (meta == null) {
      throw new IOException("Key " + name + " not found");
    }
    if (meta.getBitLength() != 8 * material.length) {
      throw new IOException("Wrong key length. Required " +
          meta.getBitLength() + ", but got " + (8 * material.length));
    }
    int nextVersion = meta.addVersion();
    credentials.addSecretKey(new Text(name), meta.serialize());
    String versionName = buildVersionName(name, nextVersion);
    credentials.addSecretKey(new Text(versionName), material);
    return new KeyVersion(name, versionName, material);
  }

  @Override
  public String toString() {
    return SCHEME_NAME + ":///";
  }

  @Override
  public synchronized void flush() {
    user.addCredentials(credentials);
  }

  public static class Factory extends KeyProviderFactory {

    @Override
    public KeyProvider createProvider(URI providerName,
                                      Configuration conf) throws IOException {
      if (SCHEME_NAME.equals(providerName.getScheme())) {
        return new UserProvider(conf);
      }
      return null;
    }
  }

  @Override
  public synchronized List<String> getKeys() throws IOException {
    List<String> list = new ArrayList<String>();
    List<Text> keys = credentials.getAllSecretKeys();
    for (Text key : keys) {
      if (key.find("@") == -1) {
        list.add(key.toString());
      }
    }
    return list;
  }

  @Override
  public synchronized List<KeyVersion> getKeyVersions(String name) throws IOException {
      List<KeyVersion> list = new ArrayList<KeyVersion>();
      Metadata km = getMetadata(name);
      if (km != null) {
        int latestVersion = km.getVersions();
        for (int i = 0; i < latestVersion; i++) {
          KeyVersion v = getKeyVersion(buildVersionName(name, i));
          if (v != null) {
            list.add(v);
          }
        }
      }
      return list;
  }
}
