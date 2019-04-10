/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ozone;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic Implementation of the OzoneFileSystem calls.
 * <p>
 * This is the minimal version which doesn't include any statistics.
 * <p>
 * For full featured version use OzoneClientAdapterImpl.
 */
public class BasicOzoneClientAdapterImpl implements OzoneClientAdapter {

  static final Logger LOG =
      LoggerFactory.getLogger(BasicOzoneClientAdapterImpl.class);

  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  private OzoneVolume volume;
  private OzoneBucket bucket;
  private ReplicationType replicationType;
  private ReplicationFactor replicationFactor;
  private boolean securityEnabled;

  /**
   * Create new OzoneClientAdapter implementation.
   *
   * @param volumeStr Name of the volume to use.
   * @param bucketStr Name of the bucket to use
   * @throws IOException In case of a problem.
   */
  public BasicOzoneClientAdapterImpl(String volumeStr, String bucketStr)
      throws IOException {
    this(createConf(), volumeStr, bucketStr);
  }

  private static OzoneConfiguration createConf() {
    ClassLoader contextClassLoader =
        Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    OzoneConfiguration conf = new OzoneConfiguration();
    Thread.currentThread().setContextClassLoader(contextClassLoader);
    return conf;
  }

  public BasicOzoneClientAdapterImpl(OzoneConfiguration conf, String volumeStr,
      String bucketStr)
      throws IOException {
    this(null, -1, conf, volumeStr, bucketStr);
  }

  public BasicOzoneClientAdapterImpl(String omHost, int omPort,
      Configuration hadoopConf, String volumeStr, String bucketStr)
      throws IOException {

    ClassLoader contextClassLoader =
        Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    OzoneConfiguration conf;
    if (hadoopConf instanceof OzoneConfiguration) {
      conf = (OzoneConfiguration) hadoopConf;
    } else {
      conf = new OzoneConfiguration(hadoopConf);
    }

    SecurityConfig secConfig = new SecurityConfig(conf);

    if (secConfig.isSecurityEnabled()) {
      this.securityEnabled = true;
    }

    try {
      String replicationTypeConf =
          conf.get(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
              OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT);

      int replicationCountConf = conf.getInt(OzoneConfigKeys.OZONE_REPLICATION,
          OzoneConfigKeys.OZONE_REPLICATION_DEFAULT);

      if (StringUtils.isNotEmpty(omHost) && omPort != -1) {
        this.ozoneClient =
            OzoneClientFactory.getRpcClient(omHost, omPort, conf);
      } else {
        this.ozoneClient =
            OzoneClientFactory.getRpcClient(conf);
      }
      objectStore = ozoneClient.getObjectStore();
      this.volume = objectStore.getVolume(volumeStr);
      this.bucket = volume.getBucket(bucketStr);
      this.replicationType = ReplicationType.valueOf(replicationTypeConf);
      this.replicationFactor = ReplicationFactor.valueOf(replicationCountConf);
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }

  }

  @Override
  public void close() throws IOException {
    ozoneClient.close();
  }

  @Override
  public InputStream readFile(String key) throws IOException {
    incrementCounter(Statistic.OBJECTS_READ);
    try {
      return bucket.readFile(key).getInputStream();
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.FILE_NOT_FOUND
          || ex.getResult() == OMException.ResultCodes.NOT_A_FILE) {
        throw new FileNotFoundException(
            ex.getResult().name() + ": " + ex.getMessage());
      } else {
        throw ex;
      }
    }
  }

  protected void incrementCounter(Statistic objectsRead) {
    //noop: Use OzoneClientAdapterImpl which supports statistics.
  }

  @Override
  public OzoneFSOutputStream createFile(String key, boolean overWrite,
      boolean recursive) throws IOException {
    incrementCounter(Statistic.OBJECTS_CREATED);
    try {
      OzoneOutputStream ozoneOutputStream = bucket
          .createFile(key, 0, replicationType, replicationFactor, overWrite,
              recursive);
      return new OzoneFSOutputStream(ozoneOutputStream.getOutputStream());
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.FILE_ALREADY_EXISTS
          || ex.getResult() == OMException.ResultCodes.NOT_A_FILE) {
        throw new FileAlreadyExistsException(
            ex.getResult().name() + ": " + ex.getMessage());
      } else {
        throw ex;
      }
    }
  }

  @Override
  public void renameKey(String key, String newKeyName) throws IOException {
    incrementCounter(Statistic.OBJECTS_RENAMED);
    bucket.renameKey(key, newKeyName);
  }

  /**
   * Helper method to create an directory specified by key name in bucket.
   *
   * @param keyName key name to be created as directory
   * @return true if the key is created, false otherwise
   */
  @Override
  public boolean createDirectory(String keyName) throws IOException {
    LOG.trace("creating dir for key:{}", keyName);
    incrementCounter(Statistic.OBJECTS_CREATED);
    try {
      bucket.createDirectory(keyName);
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.FILE_ALREADY_EXISTS) {
        throw new FileAlreadyExistsException(e.getMessage());
      }
      throw e;
    }
    return true;
  }

  /**
   * Helper method to delete an object specified by key name in bucket.
   *
   * @param keyName key name to be deleted
   * @return true if the key is deleted, false otherwise
   */
  @Override
  public boolean deleteObject(String keyName) {
    LOG.trace("issuing delete for key" + keyName);
    try {
      incrementCounter(Statistic.OBJECTS_DELETED);
      bucket.deleteKey(keyName);
      return true;
    } catch (IOException ioe) {
      LOG.error("delete key failed " + ioe.getMessage());
      return false;
    }
  }

  public OzoneFileStatus getFileStatus(String pathKey) throws IOException {
    try {
      incrementCounter(Statistic.OBJECTS_QUERY);
      return bucket.getFileStatus(pathKey);
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
        throw new
            FileNotFoundException(pathKey + ": No such file or directory!");
      }
      throw e;
    }
  }

  @Override
  public Iterator<BasicKeyInfo> listKeys(String pathKey) {
    incrementCounter(Statistic.OBJECTS_LIST);
    return new IteratorAdapter(bucket.listKeys(pathKey));
  }

  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    if (!securityEnabled) {
      return null;
    }
    Token<OzoneTokenIdentifier> token = ozoneClient.getObjectStore()
        .getDelegationToken(renewer == null ? null : new Text(renewer));
    token.setKind(OzoneTokenIdentifier.KIND_NAME);
    return token;

  }

  @Override
  public KeyProvider getKeyProvider() throws IOException {
    return objectStore.getKeyProvider();
  }

  @Override
  public URI getKeyProviderUri() throws IOException {
    return objectStore.getKeyProviderUri();
  }

  @Override
  public String getCanonicalServiceName() {
    return objectStore.getCanonicalServiceName();
  }

  /**
   * Ozone Delegation Token Renewer.
   */
  @InterfaceAudience.Private
  public static class Renewer extends TokenRenewer {

    //Ensure that OzoneConfiguration files are loaded before trying to use
    // the renewer.
    static {
      OzoneConfiguration.activate();
    }

    public Text getKind() {
      return OzoneTokenIdentifier.KIND_NAME;
    }

    @Override
    public boolean handleKind(Text kind) {
      return getKind().equals(kind);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }

    @Override
    public long renew(Token<?> token, Configuration conf)
        throws IOException, InterruptedException {
      Token<OzoneTokenIdentifier> ozoneDt =
          (Token<OzoneTokenIdentifier>) token;
      OzoneClient ozoneClient =
          OzoneClientFactory.getRpcClient(conf);
      return ozoneClient.getObjectStore().renewDelegationToken(ozoneDt);
    }

    @Override
    public void cancel(Token<?> token, Configuration conf)
        throws IOException, InterruptedException {
      Token<OzoneTokenIdentifier> ozoneDt =
          (Token<OzoneTokenIdentifier>) token;
      OzoneClient ozoneClient =
          OzoneClientFactory.getRpcClient(conf);
      ozoneClient.getObjectStore().cancelDelegationToken(ozoneDt);
    }
  }

  /**
   * Adapter to convert OzoneKey to a safe and simple Key implementation.
   */
  public static class IteratorAdapter implements Iterator<BasicKeyInfo> {

    private Iterator<? extends OzoneKey> original;

    public IteratorAdapter(Iterator<? extends OzoneKey> listKeys) {
      this.original = listKeys;
    }

    @Override
    public boolean hasNext() {
      return original.hasNext();
    }

    @Override
    public BasicKeyInfo next() {
      OzoneKey next = original.next();
      if (next == null) {
        return null;
      } else {
        return new BasicKeyInfo(
            next.getName(),
            next.getModificationTime(),
            next.getDataSize()
        );
      }
    }
  }
}
