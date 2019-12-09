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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
/**
 * A history server state storage implementation that supports any persistent
 * storage that adheres to the FileSystem interface.
 */
public class HistoryServerFileSystemStateStoreService
    extends HistoryServerStateStoreService {

  public static final Logger LOG =
      LoggerFactory.getLogger(HistoryServerFileSystemStateStoreService.class);

  private static final String ROOT_STATE_DIR_NAME = "HistoryServerState";
  private static final String TOKEN_STATE_DIR_NAME = "tokens";
  private static final String TOKEN_KEYS_DIR_NAME = "keys";
  private static final String TOKEN_BUCKET_DIR_PREFIX = "tb_";
  private static final String TOKEN_BUCKET_NAME_FORMAT =
      TOKEN_BUCKET_DIR_PREFIX + "%03d";
  private static final String TOKEN_MASTER_KEY_FILE_PREFIX = "key_";
  private static final String TOKEN_FILE_PREFIX = "token_";
  private static final String TMP_FILE_PREFIX = "tmp-";
  private static final String UPDATE_TMP_FILE_PREFIX = "update-";
  private static final FsPermission DIR_PERMISSIONS =
      new FsPermission((short)0700);
  private static final FsPermission FILE_PERMISSIONS = Shell.WINDOWS
      ? new FsPermission((short) 0700) : new FsPermission((short) 0400);
  private static final int NUM_TOKEN_BUCKETS = 1000;

  private FileSystem fs;
  private Path rootStatePath;
  private Path tokenStatePath;
  private Path tokenKeysStatePath;

  @Override
  protected void initStorage(Configuration conf)
      throws IOException {
    final String storeUri = conf.get(JHAdminConfig.MR_HS_FS_STATE_STORE_URI);
    if (storeUri == null) {
      throw new IOException("No store location URI configured in " +
          JHAdminConfig.MR_HS_FS_STATE_STORE_URI);
    }

    LOG.info("Using " + storeUri + " for history server state storage");
    rootStatePath = new Path(storeUri, ROOT_STATE_DIR_NAME);
  }

  @Override
  protected void startStorage() throws IOException {
    fs = createFileSystem();
    createDir(rootStatePath);
    tokenStatePath = new Path(rootStatePath, TOKEN_STATE_DIR_NAME);
    createDir(tokenStatePath);
    tokenKeysStatePath = new Path(tokenStatePath, TOKEN_KEYS_DIR_NAME);
    createDir(tokenKeysStatePath);
    for (int i=0; i < NUM_TOKEN_BUCKETS; ++i) {
      createDir(getTokenBucketPath(i));
    }
  }

  FileSystem createFileSystem() throws IOException {
    return rootStatePath.getFileSystem(getConfig());
  }

  @Override
  protected void closeStorage() throws IOException {
    // don't close the filesystem as it's part of the filesystem cache
    // and other clients may still be using it
  }

  @Override
  public HistoryServerState loadState() throws IOException {
    LOG.info("Loading history server state from " + rootStatePath);
    HistoryServerState state = new HistoryServerState();
    loadTokenState(state);
    return state;
  }

  @Override
  public void storeToken(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing token " + tokenId.getSequenceNumber());
    }

    Path tokenPath = getTokenPath(tokenId);
    if (fs.exists(tokenPath)) {
      throw new IOException(tokenPath + " already exists");
    }

    createNewFile(tokenPath, buildTokenData(tokenId, renewDate));
  }

  @Override
  public void updateToken(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating token " + tokenId.getSequenceNumber());
    }

    // Files cannot be atomically replaced, therefore we write a temporary
    // update file, remove the original token file, then rename the update
    // file to the token file. During recovery either the token file will be
    // used or if that is missing and an update file is present then the
    // update file is used.
    Path tokenPath = getTokenPath(tokenId);
    Path tmp = new Path(tokenPath.getParent(),
        UPDATE_TMP_FILE_PREFIX + tokenPath.getName());
    writeFile(tmp, buildTokenData(tokenId, renewDate));
    try {
      deleteFile(tokenPath);
    } catch (IOException e) {
      fs.delete(tmp, false);
      throw e;
    }
    if (!fs.rename(tmp, tokenPath)) {
      throw new IOException("Could not rename " + tmp + " to " + tokenPath);
    }
  }

  @Override
  public void removeToken(MRDelegationTokenIdentifier tokenId)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing token " + tokenId.getSequenceNumber());
    }
    deleteFile(getTokenPath(tokenId));
  }

  @Override
  public void storeTokenMasterKey(DelegationKey key) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing master key " + key.getKeyId());
    }

    Path keyPath = new Path(tokenKeysStatePath,
        TOKEN_MASTER_KEY_FILE_PREFIX + key.getKeyId());
    if (fs.exists(keyPath)) {
      throw new FileAlreadyExistsException(keyPath + " already exists");
    }

    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      key.write(dataStream);
      dataStream.close();
      dataStream = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }

    createNewFile(keyPath, memStream.toByteArray());
  }

  @Override
  public void removeTokenMasterKey(DelegationKey key)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing master key " + key.getKeyId());
    }

    Path keyPath = new Path(tokenKeysStatePath,
        TOKEN_MASTER_KEY_FILE_PREFIX + key.getKeyId());
    deleteFile(keyPath);
  }

  private static int getBucketId(MRDelegationTokenIdentifier tokenId) {
    return tokenId.getSequenceNumber() % NUM_TOKEN_BUCKETS;
  }

  private Path getTokenBucketPath(int bucketId) {
    return new Path(tokenStatePath,
        String.format(TOKEN_BUCKET_NAME_FORMAT, bucketId));
  }

  private Path getTokenPath(MRDelegationTokenIdentifier tokenId) {
    Path bucketPath = getTokenBucketPath(getBucketId(tokenId));
    return new Path(bucketPath,
        TOKEN_FILE_PREFIX + tokenId.getSequenceNumber());
  }

  private void createDir(Path dir) throws IOException {
    try {
      FileStatus status = fs.getFileStatus(dir);
      if (!status.isDirectory()) {
        throw new FileAlreadyExistsException("Unexpected file in store: "
            + dir);
      }
      if (!status.getPermission().equals(DIR_PERMISSIONS)) {
        fs.setPermission(dir, DIR_PERMISSIONS);
      }
    } catch (FileNotFoundException e) {
      fs.mkdirs(dir, DIR_PERMISSIONS);
    }
  }

  private void createNewFile(Path file, byte[] data)
      throws IOException {
    Path tmp = new Path(file.getParent(), TMP_FILE_PREFIX + file.getName());
    writeFile(tmp, data);
    try {
      if (!fs.rename(tmp, file)) {
        throw new IOException("Could not rename " + tmp + " to " + file);
      }
    } catch (IOException e) {
      fs.delete(tmp, false);
      throw e;
    }
  }

  private void writeFile(Path file, byte[] data) throws IOException {
    final int WRITE_BUFFER_SIZE = 4096;
    FSDataOutputStream out = fs.create(file, FILE_PERMISSIONS, true,
        WRITE_BUFFER_SIZE, fs.getDefaultReplication(file),
        fs.getDefaultBlockSize(file), null);
    try {
      try {
        out.write(data);
        out.close();
        out = null;
      } finally {
        IOUtils.cleanupWithLogger(LOG, out);
      }
    } catch (IOException e) {
      fs.delete(file, false);
      throw e;
    }
  }

  private byte[] readFile(Path file, long numBytes) throws IOException {
    byte[] data = new byte[(int)numBytes];
    FSDataInputStream in = fs.open(file);
    try {
      in.readFully(data);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    return data;
  }

  private void deleteFile(Path file) throws IOException {
    boolean deleted;
    try {
      deleted = fs.delete(file, false);
    } catch (FileNotFoundException e) {
      deleted = true;
    }
    if (!deleted) {
      throw new IOException("Unable to delete " + file);
    }
  }

  private byte[] buildTokenData(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      tokenId.write(dataStream);
      dataStream.writeLong(renewDate);
      dataStream.close();
      dataStream = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }
    return memStream.toByteArray();
  }

  private void loadTokenMasterKey(HistoryServerState state, Path keyFile,
      long numKeyFileBytes) throws IOException {
    DelegationKey key = new DelegationKey();
    byte[] keyData = readFile(keyFile, numKeyFileBytes);
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(keyData));
    try {
      key.readFields(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    state.tokenMasterKeyState.add(key);
  }

  private void loadTokenFromBucket(int bucketId,
      HistoryServerState state, Path tokenFile, long numTokenFileBytes)
          throws IOException {
    MRDelegationTokenIdentifier token =
        loadToken(state, tokenFile, numTokenFileBytes);
    int tokenBucketId = getBucketId(token);
    if (tokenBucketId != bucketId) {
      throw new IOException("Token " + tokenFile
          + " should be in bucket " + tokenBucketId + ", found in bucket "
          + bucketId);
    }
  }

  private MRDelegationTokenIdentifier loadToken(HistoryServerState state,
      Path tokenFile, long numTokenFileBytes) throws IOException {
    MRDelegationTokenIdentifier tokenId = new MRDelegationTokenIdentifier();
    long renewDate;
    byte[] tokenData = readFile(tokenFile, numTokenFileBytes);
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(tokenData));
    try {
      tokenId.readFields(in);
      renewDate = in.readLong();
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    state.tokenState.put(tokenId, renewDate);
    return tokenId;
  }

  private int loadTokensFromBucket(HistoryServerState state, Path bucket)
      throws IOException {
    String numStr =
        bucket.getName().substring(TOKEN_BUCKET_DIR_PREFIX.length());
    final int bucketId = Integer.parseInt(numStr);
    int numTokens = 0;
    FileStatus[] tokenStats = fs.listStatus(bucket);
    Set<String> loadedTokens = new HashSet<String>(tokenStats.length);
    for (FileStatus stat : tokenStats) {
      String name = stat.getPath().getName();
      if (name.startsWith(TOKEN_FILE_PREFIX)) {
        loadTokenFromBucket(bucketId, state, stat.getPath(), stat.getLen());
        loadedTokens.add(name);
        ++numTokens;
      } else if (name.startsWith(UPDATE_TMP_FILE_PREFIX)) {
        String tokenName = name.substring(UPDATE_TMP_FILE_PREFIX.length());
        if (loadedTokens.contains(tokenName)) {
          // already have the token, update may be partial so ignore it
          fs.delete(stat.getPath(), false);
        } else {
          // token is missing, so try to parse the update temp file
          loadTokenFromBucket(bucketId, state, stat.getPath(), stat.getLen());
          fs.rename(stat.getPath(),
              new Path(stat.getPath().getParent(), tokenName));
          loadedTokens.add(tokenName);
          ++numTokens;
        }
      } else if (name.startsWith(TMP_FILE_PREFIX)) {
        // cleanup incomplete temp files
        fs.delete(stat.getPath(), false);
      } else {
        LOG.warn("Skipping unexpected file in history server token bucket: "
            + stat.getPath());
      }
    }
    return numTokens;
  }

  private int loadKeys(HistoryServerState state) throws IOException {
    FileStatus[] stats = fs.listStatus(tokenKeysStatePath);
    int numKeys = 0;
    for (FileStatus stat : stats) {
      String name = stat.getPath().getName();
      if (name.startsWith(TOKEN_MASTER_KEY_FILE_PREFIX)) {
        loadTokenMasterKey(state, stat.getPath(), stat.getLen());
        ++numKeys;
      } else {
        LOG.warn("Skipping unexpected file in history server token state: "
            + stat.getPath());
      }
    }
    return numKeys;
  }

  private int loadTokens(HistoryServerState state) throws IOException {
    FileStatus[] stats = fs.listStatus(tokenStatePath);
    int numTokens = 0;
    for (FileStatus stat : stats) {
      String name = stat.getPath().getName();
      if (name.startsWith(TOKEN_BUCKET_DIR_PREFIX)) {
        numTokens += loadTokensFromBucket(state, stat.getPath());
      } else if (name.equals(TOKEN_KEYS_DIR_NAME)) {
        // key loading is done elsewhere
        continue;
      } else {
        LOG.warn("Skipping unexpected file in history server token state: "
            + stat.getPath());
      }
    }
    return numTokens;
  }

  private void loadTokenState(HistoryServerState state) throws IOException {
    int numKeys = loadKeys(state);
    int numTokens = loadTokens(state);
    LOG.info("Loaded " + numKeys + " master keys and " + numTokens
        + " tokens from " + tokenStatePath);
  }
}
