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
import org.apache.hadoop.fs.tosfs.object.exceptions.InvalidObjectKeyException;
import org.apache.hadoop.fs.tosfs.util.LazyReload;
import org.apache.hadoop.fs.tosfs.object.exceptions.NotAppendableException;
import org.apache.hadoop.fs.tosfs.object.request.ListObjectsRequest;
import org.apache.hadoop.fs.tosfs.object.response.ListObjectsResponse;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface ObjectStorage extends Closeable {
  String EMPTY_DELIMITER = "";

  /**
   * @return Scheme of the object storage.
   */
  String scheme();

  /**
   * @return null if bucket doesn't exist.
   */
  BucketInfo bucket();

  /**
   * Initialize the Object storage, according to the properties.
   *
   * @param conf   to initialize the {@link ObjectStorage}
   * @param bucket the corresponding bucket name, each object store has one bucket.
   */
  void initialize(Configuration conf, String bucket);

  /**
   * @return storage conf
   */
  Configuration conf();

  default ObjectContent get(String key) {
    return get(key, 0, -1);
  }

  /**
   * Get the data for the given object specified by key.
   * Throw {@link RuntimeException} if object key doesn't exist.
   * Throw {@link RuntimeException} if object key is null or empty.
   *
   * @param key     the object key.
   * @param offset  the offset to start read.
   * @param limit   the max length to read.
   * @return {@link InputStream} to read the object content.
   */
  ObjectContent get(String key, long offset, long limit);

  default byte[] put(String key, byte[] data) {
    return put(key, data, 0, data.length);
  }

  default byte[] put(String key, byte[] data, int off, int len) {
    return put(key, () -> new ByteArrayInputStream(data, off, len), len);
  }

  /**
   * Put data read from a reader to an object specified by key. The implementation must ensure to
   * close the stream created by stream provider after finishing stream operation.
   * Throw {@link RuntimeException} if object key is null or empty.
   *
   * @param key            for the object.
   * @param streamProvider the binary input stream provider that create input stream to write.
   * @param contentLength  the content length, if the actual data is bigger than content length, the
   *                       object can be created, but the object data will be truncated to the given
   *                       content length, if the actual data is smaller than content length, will
   *                       create object failed with unexpect end of IOException.
   * @return the checksum of uploaded object
   */
  byte[] put(String key, InputStreamProvider streamProvider, long contentLength);

  default byte[] append(String key, byte[] data) {
    return append(key, data, 0, data.length);
  }

  default byte[] append(String key, byte[] data, int off, int len) {
    return append(key, () -> new ByteArrayInputStream(data, off, len), len);
  }

  /**
   * Append data read from a reader to an object specified by key. If the object exists, data will
   * be appended to the tail. Otherwise, the object will be created and data will be written to it.
   * Content length could be zero if object exists. If the object doesn't exist and content length
   * is zero, a {@link NotAppendableException} will be thrown.
   * <p>
   * The first one wins if there are concurrent appends.
   * <p>
   * The implementation must ensure to close the stream created by stream provider after finishing
   * stream operation.
   * Throw {@link RuntimeException} if object key is null or empty.
   *
   * @param key            for the object.
   * @param streamProvider the binary input stream provider that create input stream to write.
   * @param contentLength  the appended content length. If the actual appended data is bigger than
   *                       content length, the object can be appended but the data to append will be
   *                       truncated to the given content length. If the actual data is smaller than
   *                       content length, append object will fail with unexpect end IOException.
   * @return the checksum of appended object.
   * @throws NotAppendableException if the object already exists and is not appendable, or the
   *                                object doesn't exist and content length is zero.
   */
  byte[] append(String key, InputStreamProvider streamProvider, long contentLength);

  /**
   * Delete an object.
   * No exception thrown if the object key doesn't exist.
   * Throw {@link RuntimeException} if object key is null or empty.
   *
   * @param key the given object key to be deleted.
   */
  void delete(String key);

  /**
   * Delete multiple keys. If one key doesn't exist, it will be treated as delete succeed, won't be
   * included in response list.
   *
   * @param keys the given object keys to be deleted
   * @return the keys delete failed
   */
  List<String> batchDelete(List<String> keys);

  /**
   * Delete all objects with the given prefix(include the prefix if the corresponding object
   * exists).
   *
   * @param prefix the prefix key.
   */
  void deleteAll(String prefix);

  /**
   * Head returns some information about the object or a null if not found.
   * Throw {@link RuntimeException} if object key is null or empty.
   * There are some differences between directory bucket and general purpose bucket:
   * <ul>
   *   <li>Assume an file object 'a/b' exists, only head("a/b") will get the meta of object 'a/b'
   *   for both general purpose bucket and directory bucket</li>
   *   <li>Assume an dir object 'a/b/' exists, regarding general purpose bucket, only head("a/b/")
   *   will get the meta of object 'a/b/', but for directory bucket, both head("a/b") and
   *   head("a/b/") will get the meta of object 'a/b/'</li>
   * </ul>
   *
   * @param key for the specified object.
   * @return {@link ObjectInfo}, null if the object does not exist.
   * @throws InvalidObjectKeyException if the object is locating under an existing file in directory
   *                                   bucket, which is not allowed.
   */
  ObjectInfo head(String key);

  /**
   * List objects according to the given {@link ListObjectsRequest}.
   *
   * @param request {@link ListObjectsRequest}
   * @return the iterable of {@link ListObjectsResponse} which contains objects and common prefixes
   */
  Iterable<ListObjectsResponse> list(ListObjectsRequest request);

  /**
   * List limited objects in a given bucket.
   *
   * @param prefix     Limits the response to keys that begin with the specified prefix.
   * @param startAfter StartAfter is where you want the object storage to start listing from.
   *                   object storage starts listing after this specified key.
   *                   StartAfter can be any key in the bucket.
   * @param limit      Limit the maximum number of response objects.
   * @return {@link ObjectInfo} the object list with matched prefix key
   */
  default Iterable<ObjectInfo> list(String prefix, String startAfter, int limit) {
    ListObjectsRequest request = ListObjectsRequest.builder()
        .prefix(prefix)
        .startAfter(startAfter)
        .maxKeys(limit)
        .delimiter(EMPTY_DELIMITER)
        .build();

    return new LazyReload<>(() -> {
      Iterator<ListObjectsResponse> iterator = list(request).iterator();
      return buf -> {
        if (!iterator.hasNext()) {
          return true;
        }
        buf.addAll(iterator.next().objects());

        return !iterator.hasNext();
      };
    });
  }

  /**
   * List all objects in a given bucket.
   *
   * @param prefix     Limits the response to keys that begin with the specified prefix.
   * @param startAfter StartAfter is where you want the object storage to start listing from.
   *                   object storage starts listing after this specified key.
   *                   StartAfter can be any key in the bucket.
   * @return {@link ObjectInfo} Iterable to iterate over the objects with matched prefix key
   *                            and StartAfter
   */
  default Iterable<ObjectInfo> listAll(String prefix, String startAfter) {
    return list(prefix, startAfter, -1);
  }

  /**
   * CreateMultipartUpload starts to upload a large object part by part.
   *
   * @param key for the specified object.
   * @return {@link MultipartUpload}.
   */
  MultipartUpload createMultipartUpload(String key);

  /**
   * UploadPart upload a part of an object. The implementation must ensure to close the stream
   * created by stream provider after finishing stream operation.
   *
   * @param key            for the specified object.
   * @param uploadId       for the multipart upload id.
   * @param partNum        upload part number.
   * @param streamProvider the stream provider to provider part stream
   * @param contentLength  the content length, if the actual data is bigger than content length, the
   *                       object can be created, but the object data will be truncated to the given
   *                       content length, if the actual data is smaller than content length, will
   *                       create object failed with unexpect end of IOException.
   * @return the uploaded part.
   */
  Part uploadPart(String key, String uploadId, int partNum, InputStreamProvider streamProvider,
      long contentLength);

  /**
   * Complete the multipart uploads with given object key and upload id.
   *
   * @param key         for the specified object.
   * @param uploadId    id of the multipart upload.
   * @param uploadParts parts to upload.
   * @return the checksum of uploaded object
   */
  byte[] completeUpload(String key, String uploadId, List<Part> uploadParts);

  /**
   * Abort a multipart upload.
   *
   * @param key      object key.
   * @param uploadId multipart upload Id.
   */
  void abortMultipartUpload(String key, String uploadId);

  /**
   * List multipart uploads under a path.
   *
   * @param prefix for uploads to abort.
   * @return Iterable to iterate over multipart unloads.
   */
  Iterable<MultipartUpload> listUploads(String prefix);

  /**
   * upload part copy with mutipart upload id.
   *
   * @param srcKey               source object key
   * @param dstKey               dest object key
   * @param uploadId             id of the multipart upload copy
   * @param partNum              part num of the multipart upload copy
   * @param copySourceRangeStart copy source range start of source object
   * @param copySourceRangeEnd   copy source range end of source object
   * @return {@link Part}.
   */
  Part uploadPartCopy(
      String srcKey, String dstKey, String uploadId, int partNum, long copySourceRangeStart,
      long copySourceRangeEnd);

  /**
   * Copy binary content from one object to another object.
   *
   * @param srcKey source object key
   * @param dstKey dest object key
   */
  void copy(String srcKey, String dstKey);

  /**
   * Atomic rename source object to dest object without any data copying.
   * Will overwrite dest object if dest object exists.
   *
   * @param srcKey source object key
   * @param dstKey dest object key
   * @throws RuntimeException if rename failed，e.g. srcKey is equal to dstKey or the source object
   *                          doesn't exist.
   */
  void rename(String srcKey, String dstKey);

  /**
   * Attach tags to specified object. This method will overwrite all existed tags with the new tags.
   * Remove all existed tags if the new tags are empty. The maximum tags number is 10.
   *
   * @param key     the key of the object key.
   * @param newTags the new tags to put.
   * @throws RuntimeException if key doesn't exist.
   */
  default void putTags(String key, Map<String, String> newTags) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " doesn't support putObjectTagging.");
  }

  /**
   * Get all attached tags of the object.
   *
   * @param key the key of the object.
   * @return map containing all tags.
   * @throws RuntimeException if key doesn't exist.
   */
  default Map<String, String> getTags(String key) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " doesn't support getObjectTagging.");
  }

  /**
   * Gets the object status for the given key.
   * It's different from {@link ObjectStorage#head(String)}, it returns object info if the key
   * exists or the prefix with value key exists.
   * <p>
   * There are three kinds of implementations:
   * <ul>
   *   <li>Uses the headObject API if the object storage support directory bucket and the requested
   *   bucket is a directory bucket, the object storage will return object directly if the file or
   *   dir exists, otherwise return null</li>
   *   <li>Uses getFileStatus API if the object storage support it, e.g. TOS. The object storage
   *   will return the object directly if the key or prefix exists, otherwise return null.</li>
   *   <li>If the object storage doesn't support above all cases, you have to try to headObject(key)
   *   at first, if the object doesn't exist, and then headObject(key + "/") later if the key
   *   doesn't end with '/', and if neither the new key doesn't exist, and then use listObjects API
   *   to check whether the prefix/key exist.</li>
   * </ul>
   *
   * @param key the object
   * @return object info if the key or prefix exists, otherwise return null.
   * @throws InvalidObjectKeyException if the object is locating under an existing file in directory
   *                                   bucket, which is not allowed.
   */
  ObjectInfo objectStatus(String key);

  /**
   * Get the object storage checksum information, including checksum algorithm name,
   * checksum type, etc.
   *
   * @return checksum information of this storage.
   */
  ChecksumInfo checksumInfo();
}
