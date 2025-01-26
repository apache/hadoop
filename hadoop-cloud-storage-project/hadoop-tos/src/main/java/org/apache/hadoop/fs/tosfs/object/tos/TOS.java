/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.object.tos;

import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TosException;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.comm.common.ACLType;
import com.volcengine.tos.comm.common.BucketType;
import com.volcengine.tos.internal.util.TypeConverter;
import com.volcengine.tos.model.bucket.HeadBucketV2Input;
import com.volcengine.tos.model.bucket.HeadBucketV2Output;
import com.volcengine.tos.model.bucket.Tag;
import com.volcengine.tos.model.object.AbortMultipartUploadInput;
import com.volcengine.tos.model.object.AppendObjectOutput;
import com.volcengine.tos.model.object.CompleteMultipartUploadV2Input;
import com.volcengine.tos.model.object.CopyObjectV2Input;
import com.volcengine.tos.model.object.CreateMultipartUploadInput;
import com.volcengine.tos.model.object.CreateMultipartUploadOutput;
import com.volcengine.tos.model.object.DeleteError;
import com.volcengine.tos.model.object.DeleteMultiObjectsV2Input;
import com.volcengine.tos.model.object.DeleteMultiObjectsV2Output;
import com.volcengine.tos.model.object.DeleteObjectInput;
import com.volcengine.tos.model.object.DeleteObjectTaggingInput;
import com.volcengine.tos.model.object.GetFileStatusInput;
import com.volcengine.tos.model.object.GetFileStatusOutput;
import com.volcengine.tos.model.object.GetObjectBasicOutput;
import com.volcengine.tos.model.object.GetObjectTaggingInput;
import com.volcengine.tos.model.object.GetObjectTaggingOutput;
import com.volcengine.tos.model.object.GetObjectV2Input;
import com.volcengine.tos.model.object.GetObjectV2Output;
import com.volcengine.tos.model.object.HeadObjectV2Input;
import com.volcengine.tos.model.object.HeadObjectV2Output;
import com.volcengine.tos.model.object.ListMultipartUploadsV2Input;
import com.volcengine.tos.model.object.ListMultipartUploadsV2Output;
import com.volcengine.tos.model.object.ListObjectsType2Input;
import com.volcengine.tos.model.object.ListObjectsType2Output;
import com.volcengine.tos.model.object.ListedCommonPrefix;
import com.volcengine.tos.model.object.ListedObjectV2;
import com.volcengine.tos.model.object.ListedUpload;
import com.volcengine.tos.model.object.ObjectMetaRequestOptions;
import com.volcengine.tos.model.object.ObjectTobeDeleted;
import com.volcengine.tos.model.object.PutObjectOutput;
import com.volcengine.tos.model.object.PutObjectTaggingInput;
import com.volcengine.tos.model.object.RenameObjectInput;
import com.volcengine.tos.model.object.TagSet;
import com.volcengine.tos.model.object.UploadPartCopyV2Input;
import com.volcengine.tos.model.object.UploadPartCopyV2Output;
import com.volcengine.tos.model.object.UploadedPartV2;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.fs.tosfs.object.BucketInfo;
import org.apache.hadoop.fs.tosfs.object.ChecksumInfo;
import org.apache.hadoop.fs.tosfs.object.ChecksumType;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.fs.tosfs.object.DirectoryStorage;
import org.apache.hadoop.fs.tosfs.object.InputStreamProvider;
import org.apache.hadoop.fs.tosfs.object.MultipartUpload;
import org.apache.hadoop.fs.tosfs.object.ObjectConstants;
import org.apache.hadoop.fs.tosfs.object.ObjectContent;
import org.apache.hadoop.fs.tosfs.object.ObjectInfo;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectUtils;
import org.apache.hadoop.fs.tosfs.object.Part;
import org.apache.hadoop.fs.tosfs.object.exceptions.InvalidObjectKeyException;
import org.apache.hadoop.fs.tosfs.object.exceptions.NotAppendableException;
import org.apache.hadoop.fs.tosfs.object.request.ListObjectsRequest;
import org.apache.hadoop.fs.tosfs.object.response.ListObjectsResponse;
import org.apache.hadoop.fs.tosfs.util.LazyReload;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.tosfs.object.tos.TOSErrorCodes.APPEND_NOT_APPENDABLE;
import static org.apache.hadoop.fs.tosfs.object.tos.TOSUtils.CHECKSUM_HEADER;
import static org.apache.hadoop.fs.tosfs.object.tos.TOSUtils.appendable;
import static org.apache.hadoop.fs.tosfs.object.tos.TOSUtils.crc64ecma;
import static org.apache.hadoop.fs.tosfs.object.tos.TOSUtils.parseChecksum;

/**
 * {@link TOS} will be initialized by the {@link ObjectStorage#initialize(Configuration, String)}.
 */
public class TOS implements DirectoryStorage {

  private static final Logger LOG = LoggerFactory.getLogger(TOS.class);
  public static final String TOS_SCHEME = "tos";

  public static final String ENV_TOS_ACCESS_KEY_ID = "TOS_ACCESS_KEY_ID";
  public static final String ENV_TOS_SECRET_ACCESS_KEY = "TOS_SECRET_ACCESS_KEY";
  public static final String ENV_TOS_SESSION_TOKEN = "TOS_SESSION_TOKEN";
  public static final String ENV_TOS_ENDPOINT = "TOS_ENDPOINT";

  private static final int NOT_FOUND_CODE = 404;
  private static final int PATH_CONFLICT_CODE = 409;
  private static final int INVALID_RANGE_CODE = 416;

  private static final int MIN_PART_SIZE = 5 * 1024 * 1024;
  private static final int MAX_PART_COUNT = 10000;

  private static final InputStream EMPTY_STREAM = new ByteArrayInputStream(new byte[0]);

  private Configuration conf;
  private String bucket;
  private DelegationClient client;
  private long maxDrainBytes;
  private int batchDeleteMaxRetries;
  private List<String> batchDeleteRetryCodes;
  private long batchDeleteRetryInterval;
  private int maxDeleteObjectsCount;
  private int listObjectsCount;
  // the max retry times during reading object content
  private int maxInputStreamRetries;
  private ACLType defaultAcl;
  private ChecksumInfo checksumInfo;
  private BucketInfo bucketInfo;

  static {
    org.apache.log4j.Logger logger = LogManager.getLogger("com.volcengine.tos");
    String logLevel = System.getProperty("tos.log.level", "WARN");

    LOG.debug("Reset the log level of com.volcengine.tos with {} ", logLevel);
    logger.setLevel(Level.toLevel(logLevel.toUpperCase(), Level.WARN));
  }

  @Override
  public void initialize(Configuration config, String bucketName) {
    this.conf = config;
    this.bucket = bucketName;
    client = new DelegationClientBuilder().conf(config).bucket(bucketName).build();
    maxDrainBytes =
        config.getLong(TosKeys.FS_TOS_MAX_DRAIN_BYTES, TosKeys.FS_TOS_MAX_DRAIN_BYTES_DEFAULT);
    batchDeleteMaxRetries = config.getInt(TosKeys.FS_TOS_BATCH_DELETE_MAX_RETRIES,
        TosKeys.FS_TOS_BATCH_DELETE_MAX_RETRIES_DEFAULT);
    batchDeleteRetryCodes = Arrays.asList(
        config.getTrimmedStrings(TosKeys.FS_TOS_BATCH_DELETE_RETRY_CODES,
            TosKeys.FS_TOS_BATCH_DELETE_RETRY_CODES_DEFAULT));
    batchDeleteRetryInterval = config.getLong(TosKeys.FS_TOS_BATCH_DELETE_RETRY_INTERVAL,
        TosKeys.FS_TOS_BATCH_DELETE_RETRY_INTERVAL_DEFAULT);
    maxDeleteObjectsCount = config.getInt(TosKeys.FS_TOS_DELETE_OBJECTS_COUNT,
        TosKeys.FS_TOS_DELETE_OBJECTS_COUNT_DEFAULT);
    listObjectsCount =
        config.getInt(TosKeys.FS_TOS_LIST_OBJECTS_COUNT, TosKeys.FS_TOS_LIST_OBJECTS_COUNT_DEFAULT);
    maxInputStreamRetries = config.getInt(TosKeys.FS_TOS_MAX_READ_OBJECT_RETRIES,
        TosKeys.FS_TOS_MAX_READ_OBJECT_RETRIES_DEFAULT);
    defaultAcl = TypeConverter.convertACLType(config.get(TosKeys.FS_TOS_ACL_DEFAULT));

    String algorithm =
        config.get(TosKeys.FS_TOS_CHECKSUM_ALGORITHM, TosKeys.FS_TOS_CHECKSUM_ALGORITHM_DEFAULT);
    ChecksumType checksumType = ChecksumType.valueOf(
        config.get(TosKeys.FS_TOS_CHECKSUM_TYPE, TosKeys.FS_TOS_CHECKSUM_TYPE_DEFAULT).toUpperCase());
    Preconditions.checkArgument(CHECKSUM_HEADER.containsKey(checksumType),
        "Checksum type %s is not supported by TOS.", checksumType.name());
    checksumInfo = new ChecksumInfo(algorithm, checksumType);

    bucketInfo = getBucketInfo(bucketName);
  }

  @Override
  public String scheme() {
    return TOS_SCHEME;
  }

  @Override
  public Configuration conf() {
    return conf;
  }

  @Override
  public BucketInfo bucket() {
    return bucketInfo;
  }

  private BucketInfo getBucketInfo(String bucketName) {
    try {
      HeadBucketV2Output res =
          client.headBucket(HeadBucketV2Input.builder().bucket(bucketName).build());

      // BUCKET_TYPE_FNS is the general purpose bucket, BUCKET_TYPE_HNS is directory bucket.
      boolean directoryBucket = BucketType.BUCKET_TYPE_HNS.equals(res.getBucketType());

      return new BucketInfo(bucketName, directoryBucket);
    } catch (TosException e) {
      if (e.getStatusCode() == NOT_FOUND_CODE) {
        return null;
      }
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  void setClient(DelegationClient client) {
    this.client = client;
  }

  private void checkAvailableClient() {
    Preconditions.checkState(client != null,
        "Encountered uninitialized ObjectStorage, call initialize(..) please.");
  }

  @Override
  public ObjectContent get(String key, long offset, long limit) {
    checkAvailableClient();
    Preconditions.checkArgument(offset >= 0, "offset is a negative number: %s", offset);

    if (limit == 0) {
      // Can not return empty stream when limit = 0, because the requested object might not exist.
      if (head(key) != null) {
        return new ObjectContent(Constants.MAGIC_CHECKSUM, EMPTY_STREAM);
      } else {
        throw new RuntimeException(String.format("Object %s doesn't exit", key));
      }
    }

    long end = limit < 0 ? -1 : offset + limit - 1;
    GetObjectFactory factory = (k, startOff, endOff) -> getObject(key, startOff, endOff);
    ChainTOSInputStream chainStream =
        new ChainTOSInputStream(factory, key, offset, end, maxDrainBytes, maxInputStreamRetries);
    return new ObjectContent(chainStream.checksum(), chainStream);
  }

  @Override
  public Iterable<ObjectInfo> listDir(String key, boolean recursive) {
    if (recursive) {
      if (bucket().isDirectory()) {
        // The directory bucket only support list object with delimiter = '/', so if we want to
        // list directory recursively, we have to list each dir step by step.
        return bfsListDir(key);
      } else {
        return listAll(key, key);
      }
    } else {
      return innerListDir(key, key, -1);
    }
  }

  private Iterable<ObjectInfo> bfsListDir(String key) {
    return new LazyReload<>(() -> {
      final Deque<String> dirQueue = new LinkedList<>();
      AtomicReference<String> continueToken = new AtomicReference<>("");
      AtomicReference<String> curDir = new AtomicReference<>(key);

      return buf -> {
        // No more objects when isTruncated is false.
        if (curDir.get() == null) {
          return true;
        }

        ListObjectsType2Input request =
            createListObjectsType2Input(curDir.get(), curDir.get(), listObjectsCount, "/",
                continueToken.get());
        ListObjectsType2Output response = client.listObjectsType2(request);

        if (response.getContents() != null) {
          for (ListedObjectV2 obj : response.getContents()) {
            buf.add(new ObjectInfo(obj.getKey(), obj.getSize(), obj.getLastModified(),
                parseChecksum(obj, checksumInfo)));
          }
        }

        if (response.getCommonPrefixes() != null) {
          for (ListedCommonPrefix prefix : response.getCommonPrefixes()) {
            buf.add(new ObjectInfo(prefix.getPrefix(), 0, new Date(), Constants.MAGIC_CHECKSUM));
            dirQueue.add(prefix.getPrefix());
          }
        }

        if (response.isTruncated()) {
          continueToken.set(response.getNextContinuationToken());
        } else {
          curDir.set(dirQueue.poll());
          continueToken.set("");
        }

        return curDir.get() == null;
      };
    });
  }

  private Iterable<ObjectInfo> innerListDir(String key, String startAfter, int limit) {
    return new LazyReload<>(() -> {
      AtomicReference<String> continueToken = new AtomicReference<>("");
      AtomicBoolean isTruncated = new AtomicBoolean(true);
      AtomicInteger remaining = new AtomicInteger(limit < 0 ? Integer.MAX_VALUE : limit);

      return buf -> {
        // No more objects when isTruncated is false.
        if (!isTruncated.get()) {
          return true;
        }

        int remainingKeys = remaining.get();
        int maxKeys = Math.min(listObjectsCount, remainingKeys);
        ListObjectsType2Input request =
            createListObjectsType2Input(key, startAfter, maxKeys, "/", continueToken.get());
        ListObjectsType2Output response = client.listObjectsType2(request);

        if (response.getContents() != null) {
          for (ListedObjectV2 obj : response.getContents()) {
            buf.add(new ObjectInfo(obj.getKey(), obj.getSize(), obj.getLastModified(),
                parseChecksum(obj, checksumInfo)));
          }
        }

        if (response.getCommonPrefixes() != null) {
          for (ListedCommonPrefix prefix : response.getCommonPrefixes()) {
            buf.add(new ObjectInfo(prefix.getPrefix(), 0, new Date(), Constants.MAGIC_CHECKSUM));
          }
        }

        isTruncated.set(response.isTruncated());
        remaining.compareAndSet(remainingKeys, remainingKeys - response.getKeyCount());
        continueToken.set(response.getNextContinuationToken());

        return !isTruncated.get();
      };
    });
  }

  @Override
  public void deleteDir(String key, boolean recursive) {
    checkAvailableClient();
    if (recursive) {
      if (conf.getBoolean(TosKeys.FS_TOS_RMR_SERVER_ENABLED,
          TosKeys.FS_FS_TOS_RMR_SERVER_ENABLED_DEFAULT)) {
        DeleteObjectInput request =
            DeleteObjectInput.builder().bucket(bucket).recursive(true).key(key).build();
        try {
          // It's a test feature, TOS SDK don't expose atomic delete dir capability currently.
          Field f = DeleteObjectInput.class.getDeclaredField("recursiveByServer");
          f.setAccessible(true);
          f.setBoolean(request, true);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        client.deleteObject(request);
      } else {
        if (conf.getBoolean(TosKeys.FS_TOS_RMR_CLIENT_ENABLE,
            TosKeys.FS_TOS_RMR_CLIENT_ENABLE_DEFAULT)) {
          client.deleteObject(
              DeleteObjectInput.builder().bucket(bucket).recursive(true).key(key).build());
        } else {
          recursiveDeleteDir(key);
        }
      }
    } else {
      delete(key);
    }
  }

  @Override
  public boolean isEmptyDir(String key) {
    checkAvailableClient();
    return !innerListDir(key, key, 1).iterator().hasNext();
  }

  public void recursiveDeleteDir(String key) {
    for (ObjectInfo obj : innerListDir(key, key, -1)) {
      if (obj.isDir()) {
        recursiveDeleteDir(obj.key());
      } else {
        delete(obj.key());
      }
    }
    delete(key);
  }

  interface GetObjectFactory {
    /**
     * Get object content for the given object key and range.
     *
     * @param key    The object key
     * @param offset The start offset of object content
     * @param end    The end offset of object content
     * @return {@link GetObjectOutput}
     */
    GetObjectOutput create(String key, long offset, long end);
  }

  public GetObjectOutput getObject(String key, long offset, long end) {
    checkAvailableClient();
    Preconditions.checkArgument(offset >= 0, "offset is a negative number: %s", offset);

    try {
      GetObjectV2Input request = GetObjectV2Input.builder().bucket(bucket).key(key)
          .options(ObjectMetaRequestOptions.builder().range(offset, end).build()).build();
      GetObjectV2Output output = client.getObject(request);

      byte[] checksum = parseChecksum(output.getRequestInfo().getHeader(), checksumInfo);
      return new GetObjectOutput(output, checksum);
    } catch (TosException e) {
      if (e instanceof TosServerException) {
        TosServerException tosException = (TosServerException) e;
        if (tosException.getStatusCode() == INVALID_RANGE_CODE) {
          ObjectInfo info = head(key);
          // if the object is empty or the requested offset is equal to object size,
          // return empty stream directly, otherwise, throw exception.
          if (info.size() == 0 || offset == info.size()) {
            return new GetObjectOutput(
                new GetObjectV2Output(new GetObjectBasicOutput(), EMPTY_STREAM), info.checksum());
          } else {
            throw new RuntimeException(e);
          }
        }
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] put(String key, InputStreamProvider streamProvider, long contentLength) {
    checkAvailableClient();
    PutObjectOutput res = client.put(bucket, key, streamProvider, contentLength, defaultAcl);
    return ObjectInfo.isDir(key) ?
        Constants.MAGIC_CHECKSUM :
        parseChecksum(res.getRequestInfo().getHeader(), checksumInfo);
  }

  @Override
  public byte[] append(String key, InputStreamProvider streamProvider, long contentLength) {
    if (bucketInfo.isDirectory()) {
      return hnsAppend(key, streamProvider, contentLength);
    } else {
      return fnsAppend(key, streamProvider, contentLength);
    }
  }

  private byte[] hnsAppend(String key, InputStreamProvider streamProvider, long contentLength) {
    checkAvailableClient();

    long offset = 0;
    String preCrc64;

    TosObjectInfo obj = innerHead(key);
    if (obj == null) {
      if (contentLength == 0) {
        throw new NotAppendableException(String.format(
            "%s is not appendable because append non-existed object with "
                + "zero byte is not supported.", key));
      }

      // In HNS, append non-existed object is not allowed. Pre-create an empty object before
      // performing appendObject.
      PutObjectOutput res = client.put(bucket, key, () -> EMPTY_STREAM, 0, defaultAcl);
      preCrc64 = res.getHashCrc64ecma();
    } else {
      if (contentLength == 0) {
        return obj.checksum();
      }
      offset = obj.size();
      preCrc64 = obj.crc64ecma();
    }

    AppendObjectOutput res =
        client.appendObject(bucket, key, streamProvider, offset, contentLength, preCrc64,
            defaultAcl);
    return ObjectInfo.isDir(key) ? Constants.MAGIC_CHECKSUM :
        parseChecksum(res.getRequestInfo().getHeader(), checksumInfo);
  }

  private byte[] fnsAppend(String key, InputStreamProvider streamProvider, long contentLength) {
    checkAvailableClient();

    TosObjectInfo obj = innerHead(key);
    if (obj != null) {
      if (!obj.appendable()) {
        throw new NotAppendableException(String.format("%s is not appendable.", key));
      }
      if (contentLength == 0) {
        return obj.checksum();
      }
    } else if (contentLength == 0) {
      throw new NotAppendableException(String.format("%s is not appendable because append"
          + " non-existed object with zero byte is not supported.", key));
    }

    long offset = obj == null ? 0 : obj.size();
    String preCrc64 = obj == null ? null : obj.crc64ecma();
    AppendObjectOutput res;
    try {
      res = client.appendObject(bucket, key, streamProvider, offset, contentLength, preCrc64,
          defaultAcl);
    } catch (TosServerException e) {
      if (e.getStatusCode() == 409 && APPEND_NOT_APPENDABLE.equals(e.getEc())) {
        throw new NotAppendableException(String.format("%s is not appendable.", key));
      }
      throw e;
    }

    return ObjectInfo.isDir(key) ?
        Constants.MAGIC_CHECKSUM :
        parseChecksum(res.getRequestInfo().getHeader(), checksumInfo);
  }

  @Override
  public void delete(String key) {
    checkAvailableClient();
    client.deleteObject(DeleteObjectInput.builder().bucket(bucket).key(key).build());
  }

  @Override
  public List<String> batchDelete(List<String> keys) {
    checkAvailableClient();
    int totalKeyCnt = keys.size();

    Preconditions.checkArgument(totalKeyCnt <= maxDeleteObjectsCount,
        "The batch delete object count should <= %s", maxDeleteObjectsCount);


    List<DeleteError> failedKeys = innerBatchDelete(keys);
    for (int retry = 1; retry < batchDeleteMaxRetries && !failedKeys.isEmpty(); retry++) {
      if (isBatchDeleteRetryable(failedKeys)) {
        try {
          Thread.sleep(batchDeleteRetryInterval);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        failedKeys = innerBatchDelete(deleteErrorKeys(failedKeys));
      } else {
        LOG.warn("{} of {} objects deleted failed, and cannot be retried, detail: {}",
            failedKeys.size(),
            totalKeyCnt,
            Joiner.on(",\n").join(failedKeys));
        break;
      }
    }

    if (!failedKeys.isEmpty()) {
      LOG.warn("{} of {} objects deleted failed after retry {} times.",
          failedKeys.size(), totalKeyCnt, batchDeleteMaxRetries);
    }

    return deleteErrorKeys(failedKeys);
  }

  @Override
  public void deleteAll(String prefix) {
    if (bucket().isDirectory()) {
      deleteDir(prefix, true);
    } else {
      Iterable<ObjectInfo> objects = listAll(prefix, "");
      ObjectUtils.deleteAllObjects(this, objects,
          conf.getInt(ConfKeys.FS_BATCH_DELETE_SIZE.key(scheme()),
              ConfKeys.FS_BATCH_DELETE_SIZE_DEFAULT));
    }
  }

  private List<DeleteError> innerBatchDelete(List<String> keys) {
    List<ObjectTobeDeleted> toBeDeleted = Lists.newArrayList();
    for (String key : keys) {
      toBeDeleted.add(ObjectTobeDeleted.builder().key(key).build());
    }

    DeleteMultiObjectsV2Output deletedRes = client.deleteMultiObjects(DeleteMultiObjectsV2Input
        .builder()
        .bucket(bucket)
        .objects(toBeDeleted)
        .build());

    return deletedRes.getErrors() == null ? Lists.newArrayList() : deletedRes.getErrors();
  }

  private boolean isBatchDeleteRetryable(List<DeleteError> failedKeys) {
    for (DeleteError errorKey : failedKeys) {
      if (batchDeleteRetryCodes.contains(errorKey.getCode())) {
        LOG.warn("Failed to delete object, which might be deleted succeed after retry, detail: {}",
            errorKey);
      } else {
        return false;
      }
    }
    return true;
  }

  private static List<String> deleteErrorKeys(List<DeleteError> errorKeys) {
    List<String> keys = Lists.newArrayList();
    for (DeleteError error : errorKeys) {
      keys.add(error.getKey());
    }
    return keys;
  }

  @Override
  public ObjectInfo head(String key) {
    return innerHead(key);
  }

  private TosObjectInfo innerHead(String key) {
    checkAvailableClient();
    try {
      HeadObjectV2Input request = HeadObjectV2Input.builder().bucket(bucket).key(key).build();
      HeadObjectV2Output response = client.headObject(request);

      // use crc64ecma/crc32c as checksum to compare object contents, don't use eTag as checksum
      // value since PUT & MPU operations have different object etags for same content.
      Map<String, String> headers = response.getRequestInfo().getHeader();
      byte[] checksum = parseChecksum(headers, checksumInfo);
      boolean isDir = bucket().isDirectory() ? response.isDirectory() : ObjectInfo.isDir(key);

      return new TosObjectInfo(key, response.getContentLength(), response.getLastModifiedInDate(),
          checksum, isDir,
          appendable(headers), crc64ecma(headers));
    } catch (TosException e) {
      if (e.getStatusCode() == NOT_FOUND_CODE) {
        return null;
      }

      if (e.getStatusCode() == PATH_CONFLICT_CODE) {
        // if a directory 'a/b/' exists in directory bucket, both headObject('a/b') and
        // headObject('a/b/') will get directory info, but the response key should be 'a/b/'.
        // But if a file 'a/b' exists in directory bucket, only headObject('a/b') will get file
        // info, headObject('a/b/') will get 409 error.
        throw new InvalidObjectKeyException(e);
      }

      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<ListObjectsResponse> list(ListObjectsRequest req) {
    return new LazyReload<>(() -> {
      AtomicReference<String> continueToken = new AtomicReference<>("");
      AtomicBoolean isTruncated = new AtomicBoolean(true);
      AtomicInteger remaining =
          new AtomicInteger(req.maxKeys() < 0 ? Integer.MAX_VALUE : req.maxKeys());

      return buf -> {
        // No more objects when isTruncated is false.
        if (!isTruncated.get()) {
          return true;
        }

        int remainingKeys = remaining.get();
        int maxKeys = Math.min(listObjectsCount, remainingKeys);
        ListObjectsType2Input request =
            createListObjectsType2Input(req.prefix(), req.startAfter(), maxKeys, req.delimiter(),
                continueToken.get());
        ListObjectsType2Output response = client.listObjectsType2(request);
        List<ObjectInfo> objects = listObjectsOutputToObjectInfos(response);
        List<String> commonPrefixes = listObjectsOutputToCommonPrefixes(response);
        buf.add(new ListObjectsResponse(objects, commonPrefixes));

        if (maxKeys < listObjectsCount) {
          isTruncated.set(false);
        } else {
          continueToken.set(response.getNextContinuationToken());
          remaining.compareAndSet(remainingKeys, remainingKeys - response.getKeyCount());
          if (remaining.get() == 0) {
            isTruncated.set(false);
          } else {
            isTruncated.set(response.isTruncated());
          }
        }
        return !isTruncated.get();
      };
    });
  }

  private List<String> listObjectsOutputToCommonPrefixes(ListObjectsType2Output listObjectsOutput) {
    if (listObjectsOutput.getCommonPrefixes() == null) {
      return Lists.newArrayList();
    }

    return listObjectsOutput.getCommonPrefixes()
        .stream()
        .map(ListedCommonPrefix::getPrefix)
        .collect(Collectors.toList());
  }

  private List<ObjectInfo> listObjectsOutputToObjectInfos(
      ListObjectsType2Output listObjectsOutput) {
    if (listObjectsOutput.getContents() == null) {
      return Lists.newArrayList();
    }
    return listObjectsOutput.getContents().stream()
        .map(obj -> new ObjectInfo(
            obj.getKey(),
            obj.getSize(),
            obj.getLastModified(),
            parseChecksum(obj, checksumInfo)))
        .collect(Collectors.toList());
  }

  private ListObjectsType2Input createListObjectsType2Input(
      String prefix, String startAfter, int maxKeys, String delimiter, String continueToken) {
    ListObjectsType2Input.ListObjectsType2InputBuilder builder = ListObjectsType2Input.builder()
        .bucket(bucket)
        .prefix(prefix)
        .startAfter(startAfter)
        .delimiter(delimiter)
        .maxKeys(maxKeys);

    if (!Strings.isNullOrEmpty(continueToken)) {
      builder.continuationToken(continueToken);
    }
    return builder.build();
  }

  @Override
  public MultipartUpload createMultipartUpload(String key) {
    checkAvailableClient();
    CreateMultipartUploadInput input = CreateMultipartUploadInput.builder()
        .bucket(bucket)
        .key(key)
        .options(createMetaOptions())
        .build();
    CreateMultipartUploadOutput output = client.createMultipartUpload(input);
    return new MultipartUpload(output.getKey(), output.getUploadID(), MIN_PART_SIZE,
        MAX_PART_COUNT);
  }

  @Override
  public Part uploadPart(
      String key, String uploadId, int partNum,
      InputStreamProvider streamProvider, long contentLength) {
    checkAvailableClient();
    return client.uploadPart(bucket, key, uploadId, partNum, streamProvider, contentLength,
        defaultAcl);
  }

  @Override
  public byte[] completeUpload(String key, String uploadId, List<Part> uploadParts) {
    checkAvailableClient();
    List<UploadedPartV2> uploadedPartsV2 = uploadParts.stream().map(
        part -> UploadedPartV2.builder()
            .etag(part.eTag())
            .partNumber(part.num())
            .size(part.size())
            .build()
    ).collect(Collectors.toList());
    CompleteMultipartUploadV2Input input = CompleteMultipartUploadV2Input.builder()
        .bucket(bucket)
        .key(key)
        .uploadID(uploadId)
        .uploadedParts(uploadedPartsV2)
        .build();
    return parseChecksum(client.completeMultipartUpload(input).getRequestInfo().getHeader(),
        checksumInfo);
  }

  @Override
  public void abortMultipartUpload(String key, String uploadId) {
    checkAvailableClient();
    AbortMultipartUploadInput input = AbortMultipartUploadInput.builder()
        .bucket(bucket)
        .key(key)
        .uploadID(uploadId)
        .build();
    client.abortMultipartUpload(input);
  }

  @Override
  public Iterable<MultipartUpload> listUploads(String prefix) {
    checkAvailableClient();
    return new LazyReload<>(() -> {
      AtomicReference<String> nextKeyMarker = new AtomicReference<>("");
      AtomicReference<String> nextUploadIdMarker = new AtomicReference<>("");
      AtomicBoolean isTruncated = new AtomicBoolean(true);
      return buf -> {
        // No more uploads when isTruncated is false.
        if (!isTruncated.get()) {
          return true;
        }
        ListMultipartUploadsV2Input input = ListMultipartUploadsV2Input.builder()
            .bucket(bucket)
            .prefix(prefix)
            .keyMarker(nextKeyMarker.get())
            .uploadIDMarker(nextUploadIdMarker.get())
            .build();
        ListMultipartUploadsV2Output output = client.listMultipartUploads(input);
        isTruncated.set(output.isTruncated());
        if (output.getUploads() != null) {
          // Fill the reloaded uploads into buffer.
          for (ListedUpload upload : output.getUploads()) {
            buf.add(new MultipartUpload(upload.getKey(), upload.getUploadID(),
                ObjectConstants.MIN_PART_SIZE, ObjectConstants.MAX_PART_COUNT));
          }
          LOG.info("Retrieve {} uploads with prefix: {}, marker: {}",
              output.getUploads().size(), nextKeyMarker.get(), nextUploadIdMarker.get());
        }
        // Refresh the nextKeyMarker and nextUploadMarker for the next reload.
        nextKeyMarker.set(output.getNextKeyMarker());
        nextUploadIdMarker.set(output.getNextUploadIdMarker());

        return !isTruncated.get();
      };
    });
  }

  @Override
  public Part uploadPartCopy(
      String srcKey, String dstKey, String uploadId, int partNum, long copySourceRangeStart,
      long copySourceRangeEnd) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(srcKey), "Source key should not be empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dstKey), "Dest key should not be empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(uploadId), "Upload ID should not be empty.");
    Preconditions.checkArgument(copySourceRangeStart >= 0, "CopySourceRangeStart must be >= 0.");
    Preconditions.checkArgument(copySourceRangeEnd >= 0, "CopySourceRangeEnd must be >= 0.");
    Preconditions.checkNotNull(copySourceRangeEnd >= copySourceRangeStart,
        "CopySourceRangeEnd must be >= copySourceRangeStart.");
    checkAvailableClient();
    UploadPartCopyV2Input input = UploadPartCopyV2Input.builder()
        .bucket(bucket)
        .key(dstKey)
        .uploadID(uploadId)
        .sourceBucket(bucket)
        .sourceKey(srcKey)
        .partNumber(partNum)
        .copySourceRange(copySourceRangeStart, copySourceRangeEnd)
        .options(createMetaOptions())
        .build();
    UploadPartCopyV2Output output = client.uploadPartCopy(input);
    return new Part(output.getPartNumber(), copySourceRangeEnd - copySourceRangeStart + 1,
        output.getEtag());
  }

  @Override
  public void copy(String srcKey, String dstKey) {
    checkAvailableClient();
    CopyObjectV2Input input = CopyObjectV2Input.builder()
        .bucket(bucket)
        .key(dstKey)
        .srcBucket(bucket)
        .srcKey(srcKey)
        .options(createMetaOptions())
        .build();
    client.copyObject(input);
  }

  private ObjectMetaRequestOptions createMetaOptions() {
    return new ObjectMetaRequestOptions().setAclType(defaultAcl);
  }

  @Override
  public void rename(String srcKey, String dstKey) {
    checkAvailableClient();
    Preconditions.checkArgument(!Objects.equals(srcKey, dstKey),
        "Cannot rename to the same object");

    RenameObjectInput request = RenameObjectInput.builder()
        .bucket(bucket)
        .key(srcKey)
        .newKey(dstKey)
        .build();
    client.renameObject(request);
  }

  // TOS allows up to 10 tags. AWS S3 allows up to 10 tags too.
  @Override
  public void putTags(String key, Map<String, String> newTags) {
    checkAvailableClient();
    List<Tag> tags = newTags.entrySet().stream()
        .map(e -> new Tag().setKey(e.getKey()).setValue(e.getValue()))
        .collect(Collectors.toList());

    if (tags.size() > 0) {
      client.putObjectTagging(createPutTagInput(bucket, key, tags));
    } else {
      client.deleteObjectTagging(createDeleteTagInput(bucket, key));
    }
  }

  @Override
  public Map<String, String> getTags(String key) {
    Map<String, String> result = new HashMap<>();
    for (Tag tag : getObjectTaggingList(key)) {
      result.put(tag.getKey(), tag.getValue());
    }
    return result;
  }

  private List<Tag> getObjectTaggingList(String key) {
    checkAvailableClient();

    GetObjectTaggingInput input = GetObjectTaggingInput.builder()
        .bucket(bucket)
        .key(key)
        .build();
    GetObjectTaggingOutput output = client.getObjectTagging(input);

    TagSet tagSet = output.getTagSet();
    if (tagSet == null || tagSet.getTags() == null) {
      return new ArrayList<>();
    }
    return tagSet.getTags();
  }

  private static PutObjectTaggingInput createPutTagInput(String bucket, String key,
      List<Tag> tags) {
    return PutObjectTaggingInput.builder()
        .bucket(bucket)
        .key(key)
        .tagSet(TagSet.builder().tags(tags).build())
        .build();
  }

  private static DeleteObjectTaggingInput createDeleteTagInput(String bucket, String key) {
    return DeleteObjectTaggingInput.builder()
        .bucket(bucket)
        .key(key)
        .build();
  }

  /**
   * Implement Hadoop FileSystem.getFileStatus semantics through
   * {@link TOSV2#getFileStatus(GetFileStatusInput)}. <br>
   *
   * The detail behavior are as follows:
   * <ul>
   *   <li>Assume object 'a/b' exists in TOS, getFileStatus("a/b") will get object('a/b') succeed,
   *   getFileStatus("a/b/") will get 404.</li>
   *   <li>Assume object 'a/b/' exists in TOS, both getFileStatus("a/b") & getFileStatus("a/b/")
   *   will get object('a/b/') succeed </li>
   *   <li>Assume object 'a/b/c' exists in TOS, both getFileStatus("a/b") & getFileStatus("a/b/")
   *   will get object('a/b/') succeed.</li>
   * </ul>
   * <p>
   * And the following is the logic of {@link TOSV2#getFileStatus(GetFileStatusInput)}: <br>
   * Step 1: Head the specified key, if the head operation is successful, the response is filled
   * with the actual object. <br>
   * Step 2: Append the key with the suffix '/' to perform list operation, if the list operation is
   * successful, the response is filled with the <strong>first object from the listing results
   * </strong>; if there are no objects, return 404. <br>
   *
   * @param key for the object.
   * @return object
   */
  private ObjectInfo getFileStatus(String key) {
    checkAvailableClient();
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "key should not be empty.");

    GetFileStatusInput input = GetFileStatusInput.builder()
        .bucket(bucket)
        .key(key)
        .build();
    try {
      GetFileStatusOutput output = client.getFileStatus(input);
      if (key.equals(output.getKey()) && !ObjectInfo.isDir(output.getKey())) {
        return new ObjectInfo(key, output.getSize(), output.getLastModifiedInDate(),
            parseChecksum(output, checksumInfo));
      } else {
        String dirKey = ObjectInfo.isDir(key) ? key : key + '/';

        // If only the prefix exists but dir object key doesn't exist, will use the current date as
        // the modified date.
        Date lastModifiedInDate =
            dirKey.equals(output.getKey()) ? output.getLastModifiedInDate() : new Date();
        return new ObjectInfo(dirKey, 0, lastModifiedInDate, Constants.MAGIC_CHECKSUM, true);
      }
    } catch (TosException e) {
      // the specified object does not exist.
      if (e.getStatusCode() == NOT_FOUND_CODE) {
        return null;
      }

      if (e.getStatusCode() == PATH_CONFLICT_CODE) {
        throw new InvalidObjectKeyException(e);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public ObjectInfo objectStatus(String key) {
    if (bucket().isDirectory()) {
      return head(key);
    } else if (conf.getBoolean(TosKeys.FS_TOS_GET_FILE_STATUS_ENABLED,
        TosKeys.FS_TOS_GET_FILE_STATUS_ENABLED_DEFAULT)) {
      return getFileStatus(key);
    } else {
      ObjectInfo obj = head(key);
      if (obj == null && !ObjectInfo.isDir(key)) {
        key = key + '/';
        obj = head(key);
      }

      if (obj == null) {
        Iterable<ObjectInfo> objs = list(key, null, 1);
        if (objs.iterator().hasNext()) {
          obj = new ObjectInfo(key, 0, new Date(0), Constants.MAGIC_CHECKSUM, true);
        }
      }

      return obj;
    }
  }

  @Override
  public ChecksumInfo checksumInfo() {
    return checksumInfo;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
