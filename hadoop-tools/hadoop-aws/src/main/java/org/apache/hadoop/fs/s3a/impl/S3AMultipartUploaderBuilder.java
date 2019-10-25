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

package org.apache.hadoop.fs.s3a.impl;

import javax.annotation.Nonnull;
import java.io.IOException;

import org.apache.hadoop.fs.MultipartUploaderBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;

/**
 * Builder for S3A multiparts.
 */
public class S3AMultipartUploaderBuilder extends
    MultipartUploaderBuilder<S3AMultipartUploader, S3AMultipartUploaderBuilder> {

  private final WriteOperationHelper writeHelper;

  private final StoreContext context;

  public S3AMultipartUploaderBuilder(
      @Nonnull final S3AFileSystem fileSystem,
      @Nonnull final WriteOperationHelper writeHelper,
      @Nonnull final StoreContext context,
      @Nonnull final Path p) {
    super(fileSystem, p);
    this.writeHelper = writeHelper;
    this.context = context;
  }

  @Override
  public S3AMultipartUploaderBuilder getThisBuilder() {
    return this;
  }

  @Override
  public S3AMultipartUploader build()
      throws IllegalArgumentException, IOException {
    return new S3AMultipartUploader(this, writeHelper, context);
  }

}
