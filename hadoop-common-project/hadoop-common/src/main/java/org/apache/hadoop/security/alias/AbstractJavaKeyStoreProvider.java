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

package org.apache.hadoop.security.alias;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.ProviderUtils;

import com.google.common.base.Charsets;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Abstract class for implementing credential providers that are based on
 * Java Keystores as the underlying credential store.
 *
 * The password for the keystore is taken from the HADOOP_CREDSTORE_PASSWORD
 * environment variable with a default of 'none'.
 *
 * It is expected that for access to credential protected resource to copy the
 * creds from the original provider into the job's Credentials object, which is
 * accessed via the UserProvider. Therefore, these providers won't be directly
 * used by MapReduce tasks.
 */
@InterfaceAudience.Private
public abstract class AbstractJavaKeyStoreProvider extends CredentialProvider {
  public static final String CREDENTIAL_PASSWORD_NAME =
      "HADOOP_CREDSTORE_PASSWORD";
  public static final String KEYSTORE_PASSWORD_FILE_KEY =
      "hadoop.security.credstore.java-keystore-provider.password-file";
  public static final String KEYSTORE_PASSWORD_DEFAULT = "none";

  private Path path;
  private final URI uri;
  private final KeyStore keyStore;
  private char[] password = null;
  private boolean changed = false;
  private Lock readLock;
  private Lock writeLock;

  protected AbstractJavaKeyStoreProvider(URI uri, Configuration conf)
      throws IOException {
    this.uri = uri;
    initFileSystem(uri, conf);
    // Get the password from the user's environment
    if (System.getenv().containsKey(CREDENTIAL_PASSWORD_NAME)) {
      password = System.getenv(CREDENTIAL_PASSWORD_NAME).toCharArray();
    }
    // if not in ENV get check for file
    if (password == null) {
      String pwFile = conf.get(KEYSTORE_PASSWORD_FILE_KEY);
      if (pwFile != null) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        URL pwdFile = cl.getResource(pwFile);
        if (pwdFile != null) {
          try (InputStream is = pwdFile.openStream()) {
            password = IOUtils.toString(is).trim().toCharArray();
          }
        }
      }
    }
    if (password == null) {
      password = KEYSTORE_PASSWORD_DEFAULT.toCharArray();
    }
    try {
      keyStore = KeyStore.getInstance("jceks");
      if (keystoreExists()) {
        stashOriginalFilePermissions();
        try (InputStream in = getInputStreamForFile()) {
          keyStore.load(in, password);
        }
      } else {
        createPermissions("700");
        // required to create an empty keystore. *sigh*
        keyStore.load(null, password);
      }
    } catch (KeyStoreException e) {
      throw new IOException("Can't create keystore", e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Can't load keystore " + getPathAsString(), e);
    } catch (CertificateException e) {
      throw new IOException("Can't load keystore " + getPathAsString(), e);
    }
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public Path getPath() {
    return path;
  }

  public void setPath(Path p) {
    this.path = p;
  }

  public char[] getPassword() {
    return password;
  }

  public void setPassword(char[] pass) {
    this.password = pass;
  }

  public boolean isChanged() {
    return changed;
  }

  public void setChanged(boolean chg) {
    this.changed = chg;
  }

  public Lock getReadLock() {
    return readLock;
  }

  public void setReadLock(Lock rl) {
    this.readLock = rl;
  }

  public Lock getWriteLock() {
    return writeLock;
  }

  public void setWriteLock(Lock wl) {
    this.writeLock = wl;
  }

  public URI getUri() {
    return uri;
  }

  public KeyStore getKeyStore() {
    return keyStore;
  }

  public Map<String, CredentialEntry> getCache() {
    return cache;
  }

  private final Map<String, CredentialEntry> cache =
      new HashMap<String, CredentialEntry>();

  protected final String getPathAsString() {
    return getPath().toString();
  }

  protected abstract String getSchemeName();

  protected abstract OutputStream getOutputStreamForKeystore()
      throws IOException;

  protected abstract boolean keystoreExists() throws IOException;

  protected abstract InputStream getInputStreamForFile() throws IOException;

  protected abstract void createPermissions(String perms) throws IOException;

  protected abstract void stashOriginalFilePermissions() throws IOException;

  protected void initFileSystem(URI keystoreUri, Configuration conf)
      throws IOException {
    path = ProviderUtils.unnestUri(keystoreUri);
  }

  @Override
  public CredentialEntry getCredentialEntry(String alias)
      throws IOException {
    readLock.lock();
    try {
      SecretKeySpec key = null;
      try {
        if (cache.containsKey(alias)) {
          return cache.get(alias);
        }
        if (!keyStore.containsAlias(alias)) {
          return null;
        }
        key = (SecretKeySpec) keyStore.getKey(alias, password);
      } catch (KeyStoreException e) {
        throw new IOException("Can't get credential " + alias + " from "
            + getPathAsString(), e);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("Can't get algorithm for credential " + alias
            + " from " + getPathAsString(), e);
      } catch (UnrecoverableKeyException e) {
        throw new IOException("Can't recover credential " + alias + " from "
            + getPathAsString(), e);
      }
      return new CredentialEntry(alias, bytesToChars(key.getEncoded()));
    } finally {
      readLock.unlock();
    }
  }

  public static char[] bytesToChars(byte[] bytes) throws IOException {
    String pass;
    pass = new String(bytes, Charsets.UTF_8);
    return pass.toCharArray();
  }

  @Override
  public List<String> getAliases() throws IOException {
    readLock.lock();
    try {
      ArrayList<String> list = new ArrayList<String>();
      String alias = null;
      try {
        Enumeration<String> e = keyStore.aliases();
        while (e.hasMoreElements()) {
          alias = e.nextElement();
          list.add(alias);
        }
      } catch (KeyStoreException e) {
        throw new IOException("Can't get alias " + alias + " from "
            + getPathAsString(), e);
      }
      return list;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public CredentialEntry createCredentialEntry(String alias, char[] credential)
      throws IOException {
    writeLock.lock();
    try {
      if (keyStore.containsAlias(alias) || cache.containsKey(alias)) {
        throw new IOException("Credential " + alias + " already exists in "
            + this);
      }
      return innerSetCredential(alias, credential);
    } catch (KeyStoreException e) {
      throw new IOException("Problem looking up credential " + alias + " in "
          + this, e);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteCredentialEntry(String name) throws IOException {
    writeLock.lock();
    try {
      try {
        if (keyStore.containsAlias(name)) {
          keyStore.deleteEntry(name);
        } else {
          throw new IOException("Credential " + name + " does not exist in "
              + this);
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

  CredentialEntry innerSetCredential(String alias, char[] material)
      throws IOException {
    writeLock.lock();
    try {
      keyStore.setKeyEntry(alias,
          new SecretKeySpec(new String(material).getBytes("UTF-8"), "AES"),
          password, null);
    } catch (KeyStoreException e) {
      throw new IOException("Can't store credential " + alias + " in " + this,
          e);
    } finally {
      writeLock.unlock();
    }
    changed = true;
    return new CredentialEntry(alias, material);
  }

  @Override
  public void flush() throws IOException {
    writeLock.lock();
    try {
      if (!changed) {
        return;
      }
      // write out the keystore
      try (OutputStream out = getOutputStreamForKeystore()) {
        keyStore.store(out, password);
      } catch (KeyStoreException e) {
        throw new IOException("Can't store keystore " + this, e);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("No such algorithm storing keystore " + this, e);
      } catch (CertificateException e) {
        throw new IOException("Certificate exception storing keystore " + this,
            e);
      }
      changed = false;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    return uri.toString();
  }
}
