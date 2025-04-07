/*
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

package org.apache.hadoop.fs.tosfs.object;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.object.request.ListObjectsRequest;
import org.apache.hadoop.fs.tosfs.object.response.ListObjectsResponse;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrefixStorage implements DirectoryStorage {
  private final ObjectStorage storage;
  private final String prefix;

  public PrefixStorage(ObjectStorage storage, String prefix) {
    this.storage = storage;
    this.prefix = prefix;
  }

  @Override
  public String scheme() {
    return storage.scheme();
  }

  @Override
  public BucketInfo bucket() {
    return storage.bucket();
  }

  @Override
  public void initialize(Configuration conf, String bucket) {
    storage.initialize(conf, bucket);
  }

  @Override
  public Configuration conf() {
    return storage.conf();
  }

  @Override
  public ObjectContent get(String key, long offset, long limit) {
    Preconditions.checkArgument(key != null && key.length() > 0,
        "Object key cannot be null or empty.");
    return storage.get(prefix + key, offset, limit);
  }

  @Override
  public byte[] put(String key, InputStreamProvider streamProvider, long contentLength) {
    Preconditions.checkArgument(key != null && key.length() > 0,
        "Object key cannot be null or empty.");
    return storage.put(prefix + key, streamProvider, contentLength);
  }

  @Override
  public byte[] append(String key, InputStreamProvider streamProvider, long contentLength) {
    Preconditions.checkArgument(key != null && key.length() > 0,
        "Object key cannot be null or empty.");
    return storage.append(prefix + key, streamProvider, contentLength);
  }

  @Override
  public void delete(String key) {
    Preconditions.checkArgument(key != null, "Object key cannot be null or empty.");
    storage.delete(prefix + key);
  }

  @Override
  public List<String> batchDelete(List<String> keys) {
    return storage.batchDelete(keys.stream().map(key -> prefix + key).collect(Collectors.toList()));
  }

  @Override
  public void deleteAll(String prefixToDelete) {
    storage.deleteAll(this.prefix + prefixToDelete);
  }

  @Override
  public ObjectInfo head(String key) {
    Preconditions.checkArgument(key != null && key.length() > 0,
        "Object key cannot be null or empty.");
    return removePrefix(storage.head(prefix + key));
  }

  private ListObjectsResponse removePrefix(ListObjectsResponse response) {
    List<ObjectInfo> objects = response.objects().stream()
        .map(this::removePrefix)
        .collect(Collectors.toList());
    List<String> commonPrefixKeys = response.commonPrefixes().stream()
        .map(this::removePrefix)
        .collect(Collectors.toList());
    return new ListObjectsResponse(objects, commonPrefixKeys);
  }

  @Override
  public Iterable<ListObjectsResponse> list(ListObjectsRequest request) {
    String startAfter = Strings.isNullOrEmpty(request.startAfter()) ?
        request.startAfter() : prefix + request.startAfter();

    ListObjectsRequest newReq = ListObjectsRequest.builder()
        .prefix(prefix + request.prefix())
        .startAfter(startAfter)
        .maxKeys(request.maxKeys())
        .delimiter(request.delimiter())
        .build();

    return Iterables.transform(storage.list(newReq), this::removePrefix);
  }

  @Override
  public MultipartUpload createMultipartUpload(String key) {
    Preconditions.checkArgument(key != null && key.length() > 0,
        "Object key cannot be null or empty.");
    return removePrefix(storage.createMultipartUpload(prefix + key));
  }

  @Override
  public Part uploadPart(
      String key, String uploadId, int partNum,
      InputStreamProvider streamProvider, long contentLength) {
    Preconditions.checkArgument(key != null && key.length() > 0,
        "Object key cannot be null or empty.");
    return storage.uploadPart(prefix + key, uploadId, partNum, streamProvider, contentLength);
  }

  @Override
  public byte[] completeUpload(String key, String uploadId, List<Part> uploadParts) {
    Preconditions.checkArgument(key != null && key.length() > 0,
        "Object key cannot be null or empty.");
    return storage.completeUpload(prefix + key, uploadId, uploadParts);
  }

  @Override
  public void abortMultipartUpload(String key, String uploadId) {
    Preconditions.checkArgument(key != null && key.length() > 0,
        "Object key cannot be null or empty.");
    storage.abortMultipartUpload(prefix + key, uploadId);
  }

  @Override
  public Iterable<MultipartUpload> listUploads(String keyPrefix) {
    return Iterables.transform(storage.listUploads(prefix + keyPrefix), this::removePrefix);
  }

  @Override
  public Part uploadPartCopy(
      String srcKey, String dstKey, String uploadId, int partNum, long copySourceRangeStart,
      long copySourceRangeEnd) {
    return storage.uploadPartCopy(prefix + srcKey, prefix + dstKey, uploadId, partNum,
        copySourceRangeStart, copySourceRangeEnd);
  }

  @Override
  public void copy(String srcKey, String dstKey) {
    storage.copy(prefix + srcKey, prefix + dstKey);
  }

  @Override
  public void rename(String srcKey, String dstKey) {
    storage.rename(prefix + srcKey, prefix + dstKey);
  }

  private ObjectInfo removePrefix(ObjectInfo o) {
    if (o == null) {
      return null;
    }
    return new ObjectInfo(removePrefix(o.key()), o.size(), o.mtime(), o.checksum(), o.isDir());
  }

  private MultipartUpload removePrefix(MultipartUpload u) {
    if (u == null) {
      return null;
    }
    return new MultipartUpload(removePrefix(u.key()), u.uploadId(), u.minPartSize(),
        u.maxPartCount());
  }

  private String removePrefix(String key) {
    if (key == null) {
      return null;
    } else if (key.startsWith(prefix)) {
      return key.substring(prefix.length());
    } else {
      return key;
    }
  }

  @Override
  public void putTags(String key, Map<String, String> newTags) {
    storage.putTags(prefix + key, newTags);
  }

  @Override
  public Map<String, String> getTags(String key) {
    return storage.getTags(prefix + key);
  }

  @Override
  public ObjectInfo objectStatus(String key) {
    Preconditions.checkArgument(key != null && !key.isEmpty(),
        "Object key cannot be null or empty.");
    return removePrefix(storage.objectStatus(prefix + key));
  }

  @Override
  public ChecksumInfo checksumInfo() {
    return storage.checksumInfo();
  }

  @Override
  public void close() throws IOException {
    storage.close();
  }

  @Override
  public Iterable<ObjectInfo> listDir(String key, boolean recursive) {
    Preconditions.checkArgument(storage instanceof DirectoryStorage);
    return Iterables.transform(((DirectoryStorage) storage).listDir(prefix + key, recursive),
        this::removePrefix);
  }

  @Override
  public void deleteDir(String key, boolean recursive) {
    Preconditions.checkArgument(storage instanceof DirectoryStorage);
    ((DirectoryStorage) storage).deleteDir(prefix + key, recursive);
  }

  @Override
  public boolean isEmptyDir(String key) {
    Preconditions.checkArgument(storage instanceof DirectoryStorage);
    return ((DirectoryStorage) storage).isEmptyDir(prefix + key);
  }
}
