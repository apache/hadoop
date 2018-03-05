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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import javax.annotation.Nullable;

/**
 * Read-specific operation context struct.
 */
public class S3AReadOpContext extends S3AOpContext {
  public S3AReadOpContext(boolean isS3GuardEnabled, Invoker invoker,
      Invoker s3guardInvoker, @Nullable FileSystem.Statistics stats,
      S3AInstrumentation instrumentation, FileStatus dstFileStatus) {
    super(isS3GuardEnabled, invoker, s3guardInvoker, stats, instrumentation,
        dstFileStatus);
  }

  public S3AReadOpContext(boolean isS3GuardEnabled, Invoker invoker,
      @Nullable FileSystem.Statistics stats, S3AInstrumentation instrumentation,
      FileStatus dstFileStatus) {
    super(isS3GuardEnabled, invoker, stats, instrumentation, dstFileStatus);
  }

  /**
   * Get invoker to use for read operations.  When S3Guard is enabled we use
   * the S3Guard invoker, which deals with things like FileNotFoundException
   * differently.
   * @return invoker to use for read codepaths
   */
  public Invoker getReadInvoker() {
    if (isS3GuardEnabled) {
      return s3guardInvoker;
    } else {
      return invoker;
    }
  }
}
