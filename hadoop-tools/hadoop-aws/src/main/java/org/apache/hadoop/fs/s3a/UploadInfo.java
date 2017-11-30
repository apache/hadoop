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

import com.amazonaws.services.s3.transfer.Upload;

/**
 * Simple struct that contains information about a S3 upload.
 */
public class UploadInfo {
  private final Upload upload;
  private final long length;

  public UploadInfo(Upload upload, long length) {
    this.upload = upload;
    this.length = length;
  }

  public Upload getUpload() {
    return upload;
  }

  public long getLength() {
    return length;
  }

}
