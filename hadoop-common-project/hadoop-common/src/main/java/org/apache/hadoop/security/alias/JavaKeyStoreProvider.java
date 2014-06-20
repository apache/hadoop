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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.ProviderUtils;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
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
 * CredentialProvider based on Java's KeyStore file format. The file may be 
 * stored in any Hadoop FileSystem using the following name mangling:
 *  jceks://hdfs@nn1.example.com/my/creds.jceks -> hdfs://nn1.example.com/my/creds.jceks
 *  jceks://file/home/larry/creds.jceks -> file:///home/larry/creds.jceks
 *
 * The password for the keystore is taken from the HADOOP_CREDSTORE_PASSWORD
 * environment variable with a default of 'none'.
 *
 * It is expected that for access to credential protected resource to copy the 
 * creds from the original provider into the job's Credentials object, which is
 * accessed via the UserProvider. Therefore, this provider won't be directly 
 * used by MapReduce tasks.
 */
@InterfaceAudience.Private
public class JavaKeyStoreProvider extends CredentialProvider {
  public static final String SCHEME_NAME = "jceks";
  public static final String CREDENTIAL_PASSWORD_NAME =
      "HADOOP_CREDSTORE_PASSWORD";
  public static final String KEYSTORE_PASSWORD_FILE_KEY =
      "hadoop.security.credstore.java-keystore-provider.password-file";
  public static final String KEYSTORE_PASSWORD_DEFAULT = "none";

  private final URI uri;
  private final Path path;
  private final FileSystem fs;
  private final FsPermission permissions;
  private final KeyStore keyStore;
  private char[] password = null;
  private boolean changed = false;
  private Lock readLock;
  private Lock writeLock;

  private final Map<String, CredentialEntry> cache = new HashMap<String, CredentialEntry>();

  private JavaKeyStoreProvider(URI uri, Configuration conf) throws IOException {
    this.uri = uri;
    path = ProviderUtils.unnestUri(uri);
    fs = path.getFileSystem(conf);
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
          InputStream is = pwdFile.openStream();
          try {
            password = IOUtils.toCharArray(is);
          } finally {
            is.close();
          }
        }
      }
    }
    if (password == null) {
      password = KEYSTORE_PASSWORD_DEFAULT.toCharArray();
    }
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
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  public CredentialEntry getCredentialEntry(String alias) throws IOException {
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
        throw new IOException("Can't get credential " + alias + " from " +
                              path, e);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("Can't get algorithm for credential " + alias + " from " +
                              path, e);
      } catch (UnrecoverableKeyException e) {
        throw new IOException("Can't recover credential " + alias + " from " + path, e);
      }
      return new CredentialEntry(alias, bytesToChars(key.getEncoded()));
    } 
    finally {
      readLock.unlock();
    }
  }
  
  public static char[] bytesToChars(byte[] bytes) {
    String pass = new String(bytes);
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
        throw new IOException("Can't get alias " + alias + " from " + path, e);
      }
      return list;
    }
    finally {
      readLock.unlock();
    }
  }

  @Override
  public CredentialEntry createCredentialEntry(String alias, char[] credential)
      throws IOException {
    try {
      if (keyStore.containsAlias(alias) || cache.containsKey(alias)) {
        throw new IOException("Credential " + alias + " already exists in " + this);
      }
    } catch (KeyStoreException e) {
      throw new IOException("Problem looking up credential " + alias + " in " + this,
          e);
    }
    return innerSetCredential(alias, credential);
  }

  @Override
  public void deleteCredentialEntry(String name) throws IOException {
    writeLock.lock();
    try {
      try {
        if (keyStore.containsAlias(name)) {
          keyStore.deleteEntry(name);
        }
        else {
          throw new IOException("Credential " + name + " does not exist in " + this);
        }
      } catch (KeyStoreException e) {
        throw new IOException("Problem removing " + name + " from " +
            this, e);
      }
      cache.remove(name);
      changed = true;
    }
    finally {
      writeLock.unlock();
    }
  }

  CredentialEntry innerSetCredential(String alias, char[] material)
      throws IOException {
    try {
      keyStore.setKeyEntry(alias, new SecretKeySpec(
          new String(material).getBytes("UTF-8"), "AES"),
          password, null);
    } catch (KeyStoreException e) {
      throw new IOException("Can't store credential " + alias + " in " + this,
          e);
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
    finally {
      writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    return uri.toString();
  }

  /**
   * The factory to create JksProviders, which is used by the ServiceLoader.
   */
  public static class Factory extends CredentialProviderFactory {
    @Override
    public CredentialProvider createProvider(URI providerName,
                                      Configuration conf) throws IOException {
      if (SCHEME_NAME.equals(providerName.getScheme())) {
        return new JavaKeyStoreProvider(providerName, conf);
      }
      return null;
    }
  }
}
