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

package org.apache.hadoop.ozone.om.request.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.UniqueId;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.TableIterator;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;


import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.NONE;

/**
 * Handles create file request.
 */
public class OMFileCreateRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMFileCreateRequest.class);
  public OMFileCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }


  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CreateFileRequest createFileRequest = getOmRequest().getCreateFileRequest();
    Preconditions.checkNotNull(createFileRequest);

    KeyArgs keyArgs = createFileRequest.getKeyArgs();

    if (keyArgs.getKeyName().length() == 0) {
      // Check if this is the root of the filesystem.
      // Not throwing exception here, as need to throw exception after
      // checking volume/bucket exists.
      return getOmRequest().toBuilder().setUserInfo(getUserInfo()).build();
    }

    long scmBlockSize = ozoneManager.getScmBlockSize();

    // NOTE size of a key is not a hard limit on anything, it is a value that
    // client should expect, in terms of current size of key. If client sets
    // a value, then this value is used, otherwise, we allocate a single
    // block which is the current size, if read by the client.
    final long requestedSize = keyArgs.getDataSize() > 0 ?
        keyArgs.getDataSize() : scmBlockSize;

    boolean useRatis = ozoneManager.shouldUseRatis();

    HddsProtos.ReplicationFactor factor = keyArgs.getFactor();
    if (factor == null) {
      factor = useRatis ? HddsProtos.ReplicationFactor.THREE :
          HddsProtos.ReplicationFactor.ONE;
    }

    HddsProtos.ReplicationType type = keyArgs.getType();
    if (type == null) {
      type = useRatis ? HddsProtos.ReplicationType.RATIS :
          HddsProtos.ReplicationType.STAND_ALONE;
    }

    // TODO: Here we are allocating block with out any check for
    //  bucket/key/volume or not and also with out any authorization checks.

    List< OmKeyLocationInfo > omKeyLocationInfoList =
        allocateBlock(ozoneManager.getScmClient(),
              ozoneManager.getBlockTokenSecretManager(), type, factor,
              new ExcludeList(), requestedSize, scmBlockSize,
              ozoneManager.getPreallocateBlocksMax(),
              ozoneManager.isGrpcBlockTokenEnabled(),
              ozoneManager.getOMNodeId());

    KeyArgs.Builder newKeyArgs = keyArgs.toBuilder()
        .setModificationTime(Time.now()).setType(type).setFactor(factor)
        .setDataSize(requestedSize);

    newKeyArgs.addAllKeyLocations(omKeyLocationInfoList.stream()
        .map(OmKeyLocationInfo::getProtobuf).collect(Collectors.toList()));

    CreateFileRequest.Builder newCreateFileRequest =
        createFileRequest.toBuilder().setKeyArgs(newKeyArgs)
            .setClientID(UniqueId.next());

    return getOmRequest().toBuilder()
        .setCreateFileRequest(newCreateFileRequest).setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    CreateFileRequest createFileRequest = getOmRequest().getCreateFileRequest();
    KeyArgs keyArgs = createFileRequest.getKeyArgs();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    // if isRecursive is true, file would be created even if parent
    // directories does not exist.
    boolean isRecursive = createFileRequest.getIsRecursive();

    // if isOverWrite is true, file would be over written.
    boolean isOverWrite = createFileRequest.getIsOverwrite();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumCreateFile();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    boolean acquiredLock = false;
    IOException exception = null;
    Optional<FileEncryptionInfo> encryptionInfo = Optional.absent();
    OmKeyInfo omKeyInfo = null;

    final List<OmKeyLocationInfo> locations = new ArrayList<>();
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, keyName);
      }

      // acquire lock
      acquiredLock = omMetadataManager.getLock().acquireLock(BUCKET_LOCK,
          volumeName, bucketName);

      OmBucketInfo bucketInfo =
          omMetadataManager.getBucketTable().get(
              omMetadataManager.getBucketKey(volumeName, bucketName));

      if (bucketInfo == null) {
        throw new OMException("Bucket " + bucketName + " not found",
            OMException.ResultCodes.BUCKET_NOT_FOUND);
      }

      if (keyName.length() == 0) {
        // Check if this is the root of the filesystem.
        throw new OMException("Can not write to directory: " + keyName,
            OMException.ResultCodes.NOT_A_FILE);
      }

      OMFileRequest.OMDirectoryResult omDirectoryResult =
          OMFileRequest.verifyFilesInPath(omMetadataManager, volumeName,
              bucketName, keyName, Paths.get(keyName));

      // Check if a file or directory exists with same key name.
      if (omDirectoryResult == FILE_EXISTS) {
        if (!isOverWrite) {
          throw new OMException("File " + keyName + " already exists",
              OMException.ResultCodes.FILE_ALREADY_EXISTS);
        }
      } else if (omDirectoryResult == DIRECTORY_EXISTS) {
        throw new OMException("Can not write to directory: " + keyName,
            OMException.ResultCodes.NOT_A_FILE);
      } else if (omDirectoryResult == FILE_EXISTS_IN_GIVENPATH) {
        throw new OMException("Can not create file: " + keyName + "as there " +
            "is already file in the given path",
            OMException.ResultCodes.NOT_A_FILE);
      }

      if (!isRecursive) {
        // We cannot create a file if complete parent directories does not exist

        // verifyFilesInPath, checks only the path and its parent directories.
        // But there may be some keys below the given path. So this method
        // checks them.

        // Example:
        // Existing keys in table
        // a/b/c/d/e
        // a/b/c/d/f
        // a/b

        // Take an example if given key to be created with isRecursive set
        // to false is "a/b/c/e".

        // There is no key in keyTable with the provided path.
        // Check in case if there are keys exist in given path. (This can
        // happen if keys are directly created using key requests.)

        // We need to do this check only in the case of non-recursive, so
        // not included the checks done in checkKeysUnderPath in
        // verifyFilesInPath method, as that method is common method for
        // directory and file create request. This also avoid's this
        // unnecessary check which is not required for those cases.
        if (omDirectoryResult == NONE ||
            omDirectoryResult == DIRECTORY_EXISTS_IN_GIVENPATH) {
          boolean canBeCreated = checkKeysUnderPath(omMetadataManager,
              volumeName, bucketName, keyName);
          if (!canBeCreated) {
            throw new OMException("Can not create file: " + keyName + "as one" +
                " of parent directory is not created",
                OMException.ResultCodes.NOT_A_FILE);
          }
        }
      }

      // do open key
      encryptionInfo = getFileEncryptionInfo(ozoneManager, bucketInfo);
      omKeyInfo = prepareKeyInfo(omMetadataManager, keyArgs,
          omMetadataManager.getOzoneKey(volumeName, bucketName,
              keyName), keyArgs.getDataSize(), locations,
          encryptionInfo.orNull());

      omClientResponse =  prepareCreateKeyResponse(keyArgs, omKeyInfo,
          locations, encryptionInfo.orNull(), exception,
          createFileRequest.getClientID(), transactionLogIndex, volumeName,
          bucketName, keyName, ozoneManager,
          OMAction.CREATE_FILE);
    } catch (IOException ex) {
      exception = ex;
      omClientResponse =  prepareCreateKeyResponse(keyArgs, omKeyInfo,
          locations, encryptionInfo.orNull(), exception,
          createFileRequest.getClientID(), transactionLogIndex,
          volumeName, bucketName, keyName, ozoneManager,
          OMAction.CREATE_FILE);
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredLock) {
        omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    return omClientResponse;
  }



  /**
   * Check if any keys exist under given path.
   * @param omMetadataManager
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @return if exists true, else false. If key name is one level path return
   * true.
   * @throws IOException
   */
  private boolean checkKeysUnderPath(OMMetadataManager omMetadataManager,
      @Nonnull String volumeName, @Nonnull String bucketName,
      @Nonnull String keyName) throws IOException {

    Path parentPath =  Paths.get(keyName).getParent();

    if (parentPath != null) {
      String dbKeyPath = omMetadataManager.getOzoneDirKey(volumeName,
          bucketName, parentPath.toString());

      // First check in key table cache.
      Iterator< Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> iterator =
          omMetadataManager.getKeyTable().cacheIterator();

      while (iterator.hasNext()) {
        Map.Entry< CacheKey< String >, CacheValue< OmKeyInfo > > entry =
            iterator.next();
        String key = entry.getKey().getCacheKey();
        OmKeyInfo omKeyInfo = entry.getValue().getCacheValue();
        // Making sure that entry is not for delete key request.
        if (key.startsWith(dbKeyPath) && omKeyInfo != null) {
          return true;
        }
      }
      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
               keyIter = omMetadataManager.getKeyTable().iterator()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.seek(dbKeyPath);


        if (kv != null) {
          // Check the entry in db is not marked for delete. This can happen
          // while entry is marked for delete, but it is not flushed to DB.
          CacheValue<OmKeyInfo> cacheValue = omMetadataManager.getKeyTable()
              .getCacheValue(new CacheKey<>(kv.getKey()));
          if (cacheValue != null) {
            if (kv.getKey().startsWith(dbKeyPath)
                && cacheValue.getCacheValue() != null) {
              return true; // we found at least one key with this db key path
            }
          } else {
            if (kv.getKey().startsWith(dbKeyPath)) {
              return true; // we found at least one key with this db key path
            }
          }
        }
      }
    } else {
      // one level key path.
      // We can safely return true, as this method is called after
      // verifyFilesInPath, so with this keyName there is no file and directory.
      return true;
    }
    return false;
  }
}
