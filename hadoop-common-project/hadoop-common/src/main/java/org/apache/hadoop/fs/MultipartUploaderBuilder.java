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

package org.apache.hadoop.fs;

import javax.annotation.Nonnull;
import java.io.IOException;

import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Builder interface for Multipart readers.
 * @param <S> MultipartUploader Generic Type.
 * @param <B> MultipartUploaderBuilder Generic Type.
 */
public interface MultipartUploaderBuilder<S extends MultipartUploader, B extends MultipartUploaderBuilder<S, B>>
    extends FSBuilder<S, B> {

  /**
   * Set permission for the file.
   * @param perm permission.
   * @return B Generics Type.
   */
  B permission(@Nonnull FsPermission perm);

  /**
   * Set the size of the buffer to be used.
   * @param bufSize buffer size.
   * @return B Generics Type.
   */
  B bufferSize(int bufSize);

  /**
   * Set replication factor.
   * @param replica replica.
   * @return B Generics Type.
   */
  B replication(short replica);

  /**
   * Set block size.
   * @param blkSize blkSize.
   * @return B Generics Type.
   */
  B blockSize(long blkSize);

  /**
   * Create an FSDataOutputStream at the specified path.
   * @return B Generics Type.
   */
  B create();

  /**
   * Set to true to overwrite the existing file.
   * Set it to false, an exception will be thrown when calling {@link #build()}
   * if the file exists.
   * @param overwrite overwrite.
   * @return B Generics Type.
   */
  B overwrite(boolean overwrite);

  /**
   * Append to an existing file (optional operation).
   * @return B Generics Type.
   */
  B append();

  /**
   * Set checksum opt.
   * @param chksumOpt chk sum opt.
   * @return B Generics Type.
   */
  B checksumOpt(@Nonnull Options.ChecksumOpt chksumOpt);

  /**
   * Create the FSDataOutputStream to write on the file system.
   *
   * @throws IllegalArgumentException if the parameters are not valid.
   * @throws IOException on errors when file system creates or appends the file.
   * @return S Generics Type.
   */
  S build() throws IllegalArgumentException, IOException;
}
