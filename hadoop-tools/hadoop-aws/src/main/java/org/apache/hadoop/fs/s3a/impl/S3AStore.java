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

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.s3a.Invoker;

/**
 * The Guarded S3A Store.
 * This is where the core operations are implemented; the "APIs", both public
 * (FileSystem, AbstractFileSystem) and the internal ones (WriteOperationHelper)
 * call through here.
 */
public interface S3AStore extends S3AService {

  void bind(
      StoreContext storeContext,
      RawS3A rawS3A,
      Invoker s3guardInvoker,
      ExecutorService unboundedThreadPool,
      LocalDirAllocator directoryAllocator);
}
