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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.PathIOException;

/**
 * Indicates the metadata associated with the given Path could not be persisted
 * to the metadata store (e.g. S3Guard / DynamoDB).  When this occurs, the
 * file itself has been successfully written to S3, but the metadata may be out
 * of sync.  The metadata can be corrected with the "s3guard import" command
 * provided by {@link org.apache.hadoop.fs.s3a.s3guard.S3GuardTool}.
 */
public class MetadataPersistenceException extends PathIOException {

  /**
   * Constructs a MetadataPersistenceException.
   * @param path path of the affected file
   * @param cause cause of the issue
   */
  public MetadataPersistenceException(String path, Throwable cause) {
    super(path, cause);
  }
}
