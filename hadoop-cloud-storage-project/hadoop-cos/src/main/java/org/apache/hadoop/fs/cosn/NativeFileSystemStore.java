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
package org.apache.hadoop.fs.cosn;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.PartETag;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * <p>
 * An abstraction for a key-based {@link File} store.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
interface NativeFileSystemStore {

  void initialize(URI uri, Configuration conf) throws IOException;

  void storeFile(String key, File file, byte[] md5Hash) throws IOException;

  void storeFile(String key, InputStream inputStream, byte[] md5Hash,
      long contentLength) throws IOException;

  void storeEmptyFile(String key) throws IOException;

  CompleteMultipartUploadResult completeMultipartUpload(
      String key, String uploadId, List<PartETag> partETagList);

  void abortMultipartUpload(String key, String uploadId);

  String getUploadId(String key);

  PartETag uploadPart(File file, String key, String uploadId, int partNum)
      throws IOException;

  PartETag uploadPart(InputStream inputStream, String key, String uploadId,
      int partNum, long partSize) throws IOException;

  FileMetadata retrieveMetadata(String key) throws IOException;

  InputStream retrieve(String key) throws IOException;

  InputStream retrieve(String key, long byteRangeStart) throws IOException;

  InputStream retrieveBlock(String key, long byteRangeStart, long byteRangeEnd)
      throws IOException;

  long getFileLength(String key) throws IOException;

  PartialListing list(String prefix, int maxListingLength) throws IOException;

  PartialListing list(String prefix, int maxListingLength,
      String priorLastKey, boolean recursive) throws IOException;

  void delete(String key) throws IOException;

  void copy(String srcKey, String dstKey) throws IOException;

  /**
   * Delete all keys with the given prefix. Used for testing.
   *
   * @throws IOException if purge is not supported
   */
  void purge(String prefix) throws IOException;

  /**
   * Diagnostic method to dump state to the console.
   *
   * @throws IOException if dump is not supported
   */
  void dump() throws IOException;

  void close();
}
