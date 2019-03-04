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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the OzoneFileSystem calls.
 */
public class OzoneClientAdapterImpl implements OzoneClientAdapter {

  static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientAdapterImpl.class);

  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  private OzoneVolume volume;
  private OzoneBucket bucket;
  private ReplicationType replicationType;
  private ReplicationFactor replicationFactor;
  private OzoneFSStorageStatistics storageStatistics;

  /**
   * Create new OzoneClientAdapter implementation.
   *
   * @param volumeStr         Name of the volume to use.
   * @param bucketStr         Name of the bucket to use
   * @param storageStatistics Storage statistic (optional, can be null)
   * @throws IOException In case of a problem.
   */
  public OzoneClientAdapterImpl(String volumeStr, String bucketStr,
      OzoneFSStorageStatistics storageStatistics) throws IOException {
    this(createConf(), volumeStr, bucketStr, storageStatistics);
  }

  /**
   * Create new OzoneClientAdapter implementation.
   *
   * @param volumeStr         Name of the volume to use.
   * @param bucketStr         Name of the bucket to use
   * @throws IOException In case of a problem.
   */
  public OzoneClientAdapterImpl(String volumeStr, String bucketStr)
      throws IOException {
    this(createConf(), volumeStr, bucketStr, null);
  }



  private static OzoneConfiguration createConf() {
    ClassLoader contextClassLoader =
        Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    OzoneConfiguration conf = new OzoneConfiguration();
    Thread.currentThread().setContextClassLoader(contextClassLoader);
    return conf;
  }

  public OzoneClientAdapterImpl(OzoneConfiguration conf, String volumeStr,
      String bucketStr, OzoneFSStorageStatistics storageStatistics)
      throws IOException {
    ClassLoader contextClassLoader =
        Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    try {
      String replicationTypeConf =
          conf.get(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
              OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT);

      int replicationCountConf = conf.getInt(OzoneConfigKeys.OZONE_REPLICATION,
          OzoneConfigKeys.OZONE_REPLICATION_DEFAULT);
      this.ozoneClient =
          OzoneClientFactory.getRpcClient(conf);
      objectStore = ozoneClient.getObjectStore();
      this.volume = objectStore.getVolume(volumeStr);
      this.bucket = volume.getBucket(bucketStr);
      this.replicationType = ReplicationType.valueOf(replicationTypeConf);
      this.replicationFactor = ReplicationFactor.valueOf(replicationCountConf);
      this.storageStatistics = storageStatistics;
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }

  }

  @Override
  public void close() throws IOException {
    ozoneClient.close();
  }

  @Override
  public InputStream createInputStream(String key) throws IOException {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(Statistic.OBJECTS_READ, 1);
    }
    return bucket.readKey(key).getInputStream();
  }

  @Override
  public OzoneFSOutputStream createKey(String key) throws IOException {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(Statistic.OBJECTS_CREATED, 1);
    }
    OzoneOutputStream ozoneOutputStream =
        bucket.createKey(key, 0, replicationType, replicationFactor,
            new HashMap<>());
    return new OzoneFSOutputStream(ozoneOutputStream.getOutputStream());
  }

  @Override
  public void renameKey(String key, String newKeyName) throws IOException {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(Statistic.OBJECTS_RENAMED, 1);
    }
    bucket.renameKey(key, newKeyName);
  }

  /**
   * Helper method to fetch the key metadata info.
   *
   * @param keyName key whose metadata information needs to be fetched
   * @return metadata info of the key
   */
  @Override
  public BasicKeyInfo getKeyInfo(String keyName) {
    try {
      if (storageStatistics != null) {
        storageStatistics.incrementCounter(Statistic.OBJECTS_QUERY, 1);
      }
      OzoneKey key = bucket.getKey(keyName);
      return new BasicKeyInfo(
          keyName,
          key.getModificationTime(),
          key.getDataSize()
      );
    } catch (IOException e) {
      LOG.trace("Key:{} does not exist", keyName);
      return null;
    }
  }

  /**
   * Helper method to check if an Ozone key is representing a directory.
   *
   * @param key key to be checked as a directory
   * @return true if key is a directory, false otherwise
   */
  @Override
  public boolean isDirectory(BasicKeyInfo key) {
    LOG.trace("key name:{} size:{}", key.getName(),
        key.getDataSize());
    return key.getName().endsWith(OZONE_URI_DELIMITER)
        && (key.getDataSize() == 0);
  }

  /**
   * Helper method to create an directory specified by key name in bucket.
   *
   * @param keyName key name to be created as directory
   * @return true if the key is created, false otherwise
   */
  @Override
  public boolean createDirectory(String keyName) {
    try {
      LOG.trace("creating dir for key:{}", keyName);
      if (storageStatistics != null) {
        storageStatistics.incrementCounter(Statistic.OBJECTS_CREATED, 1);
      }
      bucket.createKey(keyName, 0, replicationType, replicationFactor,
          new HashMap<>()).close();
      return true;
    } catch (IOException ioe) {
      LOG.error("create key failed for key:{}", keyName, ioe);
      return false;
    }
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
      if (storageStatistics != null) {
        storageStatistics.incrementCounter(Statistic.OBJECTS_DELETED, 1);
      }
      bucket.deleteKey(keyName);
      return true;
    } catch (IOException ioe) {
      LOG.error("delete key failed " + ioe.getMessage());
      return false;
    }
  }

  @Override
  public long getCreationTime() {
    return bucket.getCreationTime();
  }

  @Override
  public boolean hasNextKey(String key) {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(Statistic.OBJECTS_LIST, 1);
    }
    return bucket.listKeys(key).hasNext();
  }

  @Override
  public Iterator<BasicKeyInfo> listKeys(String pathKey) {
    if (storageStatistics != null) {
      storageStatistics.incrementCounter(Statistic.OBJECTS_LIST, 1);
    }
    return new IteratorAdapter(bucket.listKeys(pathKey));
  }

  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(String renewer)
      throws IOException {
    Token<OzoneTokenIdentifier> token =
        ozoneClient.getObjectStore().getDelegationToken(new Text(renewer));
    token.setKind(OzoneTokenIdentifier.KIND_NAME);
    return token;
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
