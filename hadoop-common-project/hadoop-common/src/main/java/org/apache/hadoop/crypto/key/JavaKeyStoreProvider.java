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

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import javax.crypto.spec.SecretKeySpec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.security.GeneralSecurityException;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * KeyProvider based on Java's KeyStore file format. The file may be stored in
 * any Hadoop FileSystem using the following name mangling:
 *  jks://hdfs@nn1.example.com/my/keys.jks -> hdfs://nn1.example.com/my/keys.jks
 *  jks://file/home/owen/keys.jks -> file:///home/owen/keys.jks
 * <p/>
 * If the <code>HADOOP_KEYSTORE_PASSWORD</code> environment variable is set,
 * its value is used as the password for the keystore.
 * <p/>
 * If the <code>HADOOP_KEYSTORE_PASSWORD</code> environment variable is not set,
 * the password for the keystore is read from file specified in the
 * {@link #KEYSTORE_PASSWORD_FILE_KEY} configuration property. The password file
 * is looked up in Hadoop's configuration directory via the classpath.
 * <p/>
 * <b>NOTE:</b> Make sure the password in the password file does not have an
 * ENTER at the end, else it won't be valid for the Java KeyStore.
 * <p/>
 * If the environment variable, nor the property are not set, the password used
 * is 'none'.
 * <p/>
 * It is expected for encrypted InputFormats and OutputFormats to copy the keys
 * from the original provider into the job's Credentials object, which is
 * accessed via the UserProvider. Therefore, this provider won't be used by
 * MapReduce tasks.
 */
@InterfaceAudience.Private
public class JavaKeyStoreProvider extends KeyProvider {
  private static final String KEY_METADATA = "KeyMetadata";
  private static final Logger LOG =
      LoggerFactory.getLogger(JavaKeyStoreProvider.class);

  public static final String SCHEME_NAME = "jceks";

  public static final String KEYSTORE_PASSWORD_FILE_KEY =
      "hadoop.security.keystore.java-keystore-provider.password-file";

  public static final String KEYSTORE_PASSWORD_ENV_VAR =
      "HADOOP_KEYSTORE_PASSWORD";
  public static final char[] KEYSTORE_PASSWORD_DEFAULT = "none".toCharArray();

  private final URI uri;
  private final Path path;
  private final FileSystem fs;
  private FsPermission permissions;
  private KeyStore keyStore;
  private char[] password;
  private boolean changed = false;
  private Lock readLock;
  private Lock writeLock;

  private final Map<String, Metadata> cache = new HashMap<String, Metadata>();

  @VisibleForTesting
  JavaKeyStoreProvider(JavaKeyStoreProvider other) {
    super(new Configuration());
    uri = other.uri;
    path = other.path;
    fs = other.fs;
    permissions = other.permissions;
    keyStore = other.keyStore;
    password = other.password;
    changed = other.changed;
    readLock = other.readLock;
    writeLock = other.writeLock;
  }

  private JavaKeyStoreProvider(URI uri, Configuration conf) throws IOException {
    super(conf);
    this.uri = uri;
    path = ProviderUtils.unnestUri(uri);
    fs = path.getFileSystem(conf);
    locateKeystore();
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  /**
   * Open up and initialize the keyStore.
   * @throws IOException If there is a problem reading the password file
   * or a problem reading the keystore.
   */
  private void locateKeystore() throws IOException {
    try {
      password = ProviderUtils.locatePassword(KEYSTORE_PASSWORD_ENV_VAR,
          getConf().get(KEYSTORE_PASSWORD_FILE_KEY));
      if (password == null) {
        password = KEYSTORE_PASSWORD_DEFAULT;
      }
      Path oldPath = constructOldPath(path);
      Path newPath = constructNewPath(path);
      keyStore = KeyStore.getInstance(SCHEME_NAME);
      FsPermission perm = null;
      if (fs.exists(path)) {
        // flush did not proceed to completion
        // _NEW should not exist
        if (fs.exists(newPath)) {
          throw new IOException(
              String.format("Keystore not loaded due to some inconsistency "
              + "('%s' and '%s' should not exist together)!!", path, newPath));
        }
        perm = tryLoadFromPath(path, oldPath);
      } else {
        perm = tryLoadIncompleteFlush(oldPath, newPath);
      }
      // Need to save off permissions in case we need to
      // rewrite the keystore in flush()
      permissions = perm;
    } catch (KeyStoreException e) {
      throw new IOException("Can't create keystore: " + e, e);
    } catch (GeneralSecurityException e) {
      throw new IOException("Can't load keystore " + path + " : " + e , e);
    }
  }

  /**
   * Try loading from the user specified path, else load from the backup
   * path in case Exception is not due to bad/wrong password.
   * @param path Actual path to load from
   * @param backupPath Backup path (_OLD)
   * @return The permissions of the loaded file
   * @throws NoSuchAlgorithmException
   * @throws CertificateException
   * @throws IOException
   */
  private FsPermission tryLoadFromPath(Path path, Path backupPath)
      throws NoSuchAlgorithmException, CertificateException,
      IOException {
    FsPermission perm = null;
    try {
      perm = loadFromPath(path, password);
      // Remove _OLD if exists
      fs.delete(backupPath, true);
      LOG.debug("KeyStore loaded successfully !!");
    } catch (IOException ioe) {
      // If file is corrupted for some reason other than
      // wrong password try the _OLD file if exits
      if (!isBadorWrongPassword(ioe)) {
        perm = loadFromPath(backupPath, password);
        // Rename CURRENT to CORRUPTED
        renameOrFail(path, new Path(path.toString() + "_CORRUPTED_"
            + System.currentTimeMillis()));
        renameOrFail(backupPath, path);
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format(
              "KeyStore loaded successfully from '%s' since '%s'"
                  + "was corrupted !!", backupPath, path));
        }
      } else {
        throw ioe;
      }
    }
    return perm;
  }

  /**
   * The KeyStore might have gone down during a flush, In which case either the
   * _NEW or _OLD files might exists. This method tries to load the KeyStore
   * from one of these intermediate files.
   * @param oldPath the _OLD file created during flush
   * @param newPath the _NEW file created during flush
   * @return The permissions of the loaded file
   * @throws IOException
   * @throws NoSuchAlgorithmException
   * @throws CertificateException
   */
  private FsPermission tryLoadIncompleteFlush(Path oldPath, Path newPath)
      throws IOException, NoSuchAlgorithmException, CertificateException {
    FsPermission perm = null;
    // Check if _NEW exists (in case flush had finished writing but not
    // completed the re-naming)
    if (fs.exists(newPath)) {
      perm = loadAndReturnPerm(newPath, oldPath);
    }
    // try loading from _OLD (An earlier Flushing MIGHT not have completed
    // writing completely)
    if ((perm == null) && fs.exists(oldPath)) {
      perm = loadAndReturnPerm(oldPath, newPath);
    }
    // If not loaded yet,
    // required to create an empty keystore. *sigh*
    if (perm == null) {
      keyStore.load(null, password);
      LOG.debug("KeyStore initialized anew successfully !!");
      perm = new FsPermission("600");
    }
    return perm;
  }

  private FsPermission loadAndReturnPerm(Path pathToLoad, Path pathToDelete)
      throws NoSuchAlgorithmException, CertificateException,
      IOException {
    FsPermission perm = null;
    try {
      perm = loadFromPath(pathToLoad, password);
      renameOrFail(pathToLoad, path);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("KeyStore loaded successfully from '%s'!!",
            pathToLoad));
      }
      fs.delete(pathToDelete, true);
    } catch (IOException e) {
      // Check for password issue : don't want to trash file due
      // to wrong password
      if (isBadorWrongPassword(e)) {
        throw e;
      }
    }
    return perm;
  }

  private boolean isBadorWrongPassword(IOException ioe) {
    // As per documentation this is supposed to be the way to figure
    // if password was correct
    if (ioe.getCause() instanceof UnrecoverableKeyException) {
      return true;
    }
    // Unfortunately that doesn't seem to work..
    // Workaround :
    if ((ioe.getCause() == null)
        && (ioe.getMessage() != null)
        && ((ioe.getMessage().contains("Keystore was tampered")) || (ioe
            .getMessage().contains("password was incorrect")))) {
      return true;
    }
    return false;
  }

  private FsPermission loadFromPath(Path p, char[] password)
      throws IOException, NoSuchAlgorithmException, CertificateException {
    try (FSDataInputStream in = fs.open(p)) {
      FileStatus s = fs.getFileStatus(p);
      keyStore.load(in, password);
      return s.getPermission();
    }
  }

  private static Path constructNewPath(Path path) {
    return new Path(path.toString() + "_NEW");
  }

  private static Path constructOldPath(Path path) {
    return new Path(path.toString() + "_OLD");
  }

  @Override
  public boolean needsPassword() throws IOException {
    return (null == ProviderUtils.locatePassword(KEYSTORE_PASSWORD_ENV_VAR,
        getConf().get(KEYSTORE_PASSWORD_FILE_KEY)));

  }

  @Override
  public String noPasswordWarning() {
    return ProviderUtils.noPasswordWarning(KEYSTORE_PASSWORD_ENV_VAR,
        KEYSTORE_PASSWORD_FILE_KEY);
  }

  @Override
  public String noPasswordError() {
    return ProviderUtils.noPasswordError(KEYSTORE_PASSWORD_ENV_VAR,
        KEYSTORE_PASSWORD_FILE_KEY);
  }

  @Override
  public KeyVersion getKeyVersion(String versionName) throws IOException {
    readLock.lock();
    try {
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
      return new KeyVersion(getBaseName(versionName), versionName, key.getEncoded());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getKeys() throws IOException {
    readLock.lock();
    try {
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
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<KeyVersion> getKeyVersions(String name) throws IOException {
    readLock.lock();
    try {
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
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Metadata getMetadata(String name) throws IOException {
    readLock.lock();
    try {
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
      } catch (ClassCastException e) {
        throw new IOException("Can't cast key for " + name + " in keystore " +
            path + " to a KeyMetadata. Key may have been added using " +
            " keytool or some other non-Hadoop method.", e);
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
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public KeyVersion createKey(String name, byte[] material,
                               Options options) throws IOException {
    Preconditions.checkArgument(name.equals(StringUtils.toLowerCase(name)),
        "Uppercase key names are unsupported: %s", name);
    writeLock.lock();
    try {
      try {
        if (keyStore.containsAlias(name) || cache.containsKey(name)) {
          throw new IOException("Key " + name + " already exists in " + this);
        }
      } catch (KeyStoreException e) {
        throw new IOException("Problem looking up key " + name + " in " + this,
            e);
      }
      Metadata meta = new Metadata(options.getCipher(), options.getBitLength(),
          options.getDescription(), options.getAttributes(), new Date(), 1);
      if (options.getBitLength() != 8 * material.length) {
        throw new IOException("Wrong key length. Required " +
            options.getBitLength() + ", but got " + (8 * material.length));
      }
      cache.put(name, meta);
      String versionName = buildVersionName(name, 0);
      return innerSetKeyVersion(name, versionName, material, meta.getCipher());
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteKey(String name) throws IOException {
    writeLock.lock();
    try {
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
    } finally {
      writeLock.unlock();
    }
  }

  KeyVersion innerSetKeyVersion(String name, String versionName, byte[] material,
                                String cipher) throws IOException {
    try {
      keyStore.setKeyEntry(versionName, new SecretKeySpec(material, cipher),
          password, null);
    } catch (KeyStoreException e) {
      throw new IOException("Can't store key " + versionName + " in " + this,
          e);
    }
    changed = true;
    return new KeyVersion(name, versionName, material);
  }

  @Override
  public KeyVersion rollNewVersion(String name,
                                    byte[] material) throws IOException {
    writeLock.lock();
    try {
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
      return innerSetKeyVersion(name, versionName, material, meta.getCipher());
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void flush() throws IOException {
    Path newPath = constructNewPath(path);
    Path oldPath = constructOldPath(path);
    Path resetPath = path;
    writeLock.lock();
    try {
      if (!changed) {
        return;
      }
      // Might exist if a backup has been restored etc.
      try {
        renameOrFail(newPath, new Path(newPath.toString()
            + "_ORPHANED_" + System.currentTimeMillis()));
      } catch (FileNotFoundException ignored) {
      }
      try {
        renameOrFail(oldPath, new Path(oldPath.toString()
            + "_ORPHANED_" + System.currentTimeMillis()));
      } catch (FileNotFoundException ignored) {
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

      // Save old File first
      boolean fileExisted = backupToOld(oldPath);
      if (fileExisted) {
        resetPath = oldPath;
      }
      // write out the keystore
      // Write to _NEW path first :
      try {
        writeToNew(newPath);
      } catch (IOException ioe) {
        // rename _OLD back to curent and throw Exception
        revertFromOld(oldPath, fileExisted);
        resetPath = path;
        throw ioe;
      }
      // Rename _NEW to CURRENT and delete _OLD
      cleanupNewAndOld(newPath, oldPath);
      changed = false;
    } catch (IOException ioe) {
      resetKeyStoreState(resetPath);
      throw ioe;
    } finally {
      writeLock.unlock();
    }
  }

  private void resetKeyStoreState(Path path) {
    LOG.debug("Could not flush Keystore.."
        + "attempting to reset to previous state !!");
    // 1) flush cache
    cache.clear();
    // 2) load keyStore from previous path
    try {
      loadFromPath(path, password);
      LOG.debug("KeyStore resetting to previously flushed state !!");
    } catch (Exception e) {
      LOG.debug("Could not reset Keystore to previous state", e);
    }
  }

  private void cleanupNewAndOld(Path newPath, Path oldPath) throws IOException {
    // Rename _NEW to CURRENT
    renameOrFail(newPath, path);
    // Delete _OLD
    fs.delete(oldPath, true);
  }

  protected void writeToNew(Path newPath) throws IOException {
    try (FSDataOutputStream out =
        FileSystem.create(fs, newPath, permissions);) {
      keyStore.store(out, password);
    } catch (KeyStoreException e) {
      throw new IOException("Can't store keystore " + this, e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(
          "No such algorithm storing keystore " + this, e);
    } catch (CertificateException e) {
      throw new IOException(
          "Certificate exception storing keystore " + this, e);
    }
  }

  protected boolean backupToOld(Path oldPath)
      throws IOException {
    try {
      renameOrFail(path, oldPath);
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  private void revertFromOld(Path oldPath, boolean fileExisted)
      throws IOException {
    if (fileExisted) {
      renameOrFail(oldPath, path);
    }
  }


  private void renameOrFail(Path src, Path dest)
      throws IOException {
    if (!fs.rename(src, dest)) {
      throw new IOException("Rename unsuccessful : "
          + String.format("'%s' to '%s'", src, dest));
    }
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
