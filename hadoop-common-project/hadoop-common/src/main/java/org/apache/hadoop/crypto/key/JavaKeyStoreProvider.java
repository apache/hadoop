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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * KeyProvider based on Java's KeyStore file format. The file may be stored in
 * any Hadoop FileSystem using the following name mangling:
 *  jks://hdfs@nn1.example.com/my/keys.jks -> hdfs://nn1.example.com/my/keys.jks
 *  jks://file/home/owen/keys.jks -> file:///home/owen/keys.jks
 *
 * The password for the keystore is taken from the HADOOP_KEYSTORE_PASSWORD
 * environment variable with a default of 'none'.
 *
 * It is expected for encrypted InputFormats and OutputFormats to copy the keys
 * from the original provider into the job's Credentials object, which is
 * accessed via the UserProvider. Therefore, this provider won't be used by
 * MapReduce tasks.
 */
@InterfaceAudience.Private
public class JavaKeyStoreProvider extends KeyProvider {
  private static final String KEY_METADATA = "KeyMetadata";
  public static final String SCHEME_NAME = "jceks";
  public static final String KEYSTORE_PASSWORD_NAME =
      "HADOOP_KEYSTORE_PASSWORD";
  public static final String KEYSTORE_PASSWORD_DEFAULT = "none";

  private final URI uri;
  private final Path path;
  private final FileSystem fs;
  private final FsPermission permissions;
  private final KeyStore keyStore;
  private final char[] password;
  private boolean changed = false;

  private final Map<String, Metadata> cache = new HashMap<String, Metadata>();

  private JavaKeyStoreProvider(URI uri, Configuration conf) throws IOException {
    this.uri = uri;
    path = unnestUri(uri);
    fs = path.getFileSystem(conf);
    // Get the password from the user's environment
    String pw = System.getenv(KEYSTORE_PASSWORD_NAME);
    if (pw == null) {
      pw = KEYSTORE_PASSWORD_DEFAULT;
    }
    password = pw.toCharArray();
    try {
      keyStore = KeyStore.getInstance(SCHEME_NAME);
      if (fs.exists(path)) {
        // save off permissions in case we need to
        // rewrite the keystore in flush()
        FileStatus s = fs.getFileStatus(path);
        permissions = s.getPermission();

        keyStore.load(fs.open(path), password);
      } else {
        permissions = new FsPermission("700");
        // required to create an empty keystore. *sigh*
        keyStore.load(null, password);
      }
    } catch (KeyStoreException e) {
      throw new IOException("Can't create keystore", e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Can't load keystore " + path, e);
    } catch (CertificateException e) {
      throw new IOException("Can't load keystore " + path, e);
    }
  }

  @Override
  public KeyVersion getKeyVersion(String versionName) throws IOException {
    SecretKeySpec key = null;
    try {
      if (!keyStore.containsAlias(versionName)) {
        return null;
      }
      key = (SecretKeySpec) keyStore.getKey(versionName, password);
    } catch (KeyStoreException e) {
      throw new IOException("Can't get key " + versionName + " from " +
                            path, e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Can't get algorithm for key " + key + " from " +
                            path, e);
    } catch (UnrecoverableKeyException e) {
      throw new IOException("Can't recover key " + key + " from " + path, e);
    }
    return new KeyVersion(versionName, key.getEncoded());
  }

  @Override
  public List<String> getKeys() throws IOException {
    ArrayList<String> list = new ArrayList<String>();
    String alias = null;
    try {
      Enumeration<String> e = keyStore.aliases();
      while (e.hasMoreElements()) {
         alias = e.nextElement();
         // only include the metadata key names in the list of names
         if (!alias.contains("@")) {
             list.add(alias);
         }
      }
    } catch (KeyStoreException e) {
      throw new IOException("Can't get key " + alias + " from " + path, e);
    }
    return list;
  }

  @Override
  public List<KeyVersion> getKeyVersions(String name) throws IOException {
    List<KeyVersion> list = new ArrayList<KeyVersion>();
    Metadata km = getMetadata(name);
    if (km != null) {
      int latestVersion = km.getVersions();
      KeyVersion v = null;
      String versionName = null;
      for (int i = 0; i < latestVersion; i++) {
        versionName = buildVersionName(name, i);
        v = getKeyVersion(versionName);
        if (v != null) {
          list.add(v);
        }
      }
    }
    return list;
  }

  @Override
  public Metadata getMetadata(String name) throws IOException {
    if (cache.containsKey(name)) {
      return cache.get(name);
    }
    try {
      if (!keyStore.containsAlias(name)) {
        return null;
      }
      Metadata meta = ((KeyMetadata) keyStore.getKey(name, password)).metadata;
      cache.put(name, meta);
      return meta;
    } catch (KeyStoreException e) {
      throw new IOException("Can't get metadata for " + name +
          " from keystore " + path, e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Can't get algorithm for " + name +
          " from keystore " + path, e);
    } catch (UnrecoverableKeyException e) {
      throw new IOException("Can't recover key for " + name +
          " from keystore " + path, e);
    }
  }

  @Override
  public KeyVersion createKey(String name, byte[] material,
                               Options options) throws IOException {
    try {
      if (keyStore.containsAlias(name) || cache.containsKey(name)) {
        throw new IOException("Key " + name + " already exists in " + this);
      }
    } catch (KeyStoreException e) {
      throw new IOException("Problem looking up key " + name + " in " + this,
          e);
    }
    Metadata meta = new Metadata(options.getCipher(), options.getBitLength(),
        new Date(), 1);
    if (options.getBitLength() != 8 * material.length) {
      throw new IOException("Wrong key length. Required " +
          options.getBitLength() + ", but got " + (8 * material.length));
    }
    cache.put(name, meta);
    String versionName = buildVersionName(name, 0);
    return innerSetKeyVersion(versionName, material, meta.getCipher());
  }

  @Override
  public void deleteKey(String name) throws IOException {
    Metadata meta = getMetadata(name);
    if (meta == null) {
      throw new IOException("Key " + name + " does not exist in " + this);
    }
    for(int v=0; v < meta.getVersions(); ++v) {
      String versionName = buildVersionName(name, v);
      try {
        if (keyStore.containsAlias(versionName)) {
          keyStore.deleteEntry(versionName);
        }
      } catch (KeyStoreException e) {
        throw new IOException("Problem removing " + versionName + " from " +
            this, e);
      }
    }
    try {
      if (keyStore.containsAlias(name)) {
        keyStore.deleteEntry(name);
      }
    } catch (KeyStoreException e) {
      throw new IOException("Problem removing " + name + " from " + this, e);
    }
    cache.remove(name);
    changed = true;
  }

  KeyVersion innerSetKeyVersion(String versionName, byte[] material,
                                String cipher) throws IOException {
    try {
      keyStore.setKeyEntry(versionName, new SecretKeySpec(material, cipher),
          password, null);
    } catch (KeyStoreException e) {
      throw new IOException("Can't store key " + versionName + " in " + this,
          e);
    }
    changed = true;
    return new KeyVersion(versionName, material);
  }

  @Override
  public KeyVersion rollNewVersion(String name,
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
    String versionName = buildVersionName(name, nextVersion);
    return innerSetKeyVersion(versionName, material, meta.getCipher());
  }

  @Override
  public void flush() throws IOException {
    if (!changed) {
      return;
    }
    // put all of the updates into the keystore
    for(Map.Entry<String, Metadata> entry: cache.entrySet()) {
      try {
        keyStore.setKeyEntry(entry.getKey(), new KeyMetadata(entry.getValue()),
            password, null);
      } catch (KeyStoreException e) {
        throw new IOException("Can't set metadata key " + entry.getKey(),e );
      }
    }
    // write out the keystore
    FSDataOutputStream out = FileSystem.create(fs, path, permissions);
    try {
      keyStore.store(out, password);
    } catch (KeyStoreException e) {
      throw new IOException("Can't store keystore " + this, e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("No such algorithm storing keystore " + this, e);
    } catch (CertificateException e) {
      throw new IOException("Certificate exception storing keystore " + this,
          e);
    }
    out.close();
    changed = false;
  }

  @Override
  public String toString() {
    return uri.toString();
  }

  /**
   * The factory to create JksProviders, which is used by the ServiceLoader.
   */
  public static class Factory extends KeyProviderFactory {
    @Override
    public KeyProvider createProvider(URI providerName,
                                      Configuration conf) throws IOException {
      if (SCHEME_NAME.equals(providerName.getScheme())) {
        return new JavaKeyStoreProvider(providerName, conf);
      }
      return null;
    }
  }

  /**
   * An adapter between a KeyStore Key and our Metadata. This is used to store
   * the metadata in a KeyStore even though isn't really a key.
   */
  public static class KeyMetadata implements Key, Serializable {
    private Metadata metadata;
    private final static long serialVersionUID = 8405872419967874451L;

    private KeyMetadata(Metadata meta) {
      this.metadata = meta;
    }

    @Override
    public String getAlgorithm() {
      return metadata.getCipher();
    }

    @Override
    public String getFormat() {
      return KEY_METADATA;
    }

    @Override
    public byte[] getEncoded() {
      return new byte[0];
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      byte[] serialized = metadata.serialize();
      out.writeInt(serialized.length);
      out.write(serialized);
    }

    private void readObject(ObjectInputStream in
                            ) throws IOException, ClassNotFoundException {
      byte[] buf = new byte[in.readInt()];
      in.readFully(buf);
      metadata = new Metadata(buf);
    }

  }
}
