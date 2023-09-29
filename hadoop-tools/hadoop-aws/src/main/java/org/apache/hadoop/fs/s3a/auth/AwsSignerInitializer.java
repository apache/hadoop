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

package org.apache.hadoop.fs.s3a.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenProvider;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Interface which can be implemented to allow initialization of any custom
 * signers which may be used by the {@link S3AFileSystem}.
 */
public interface AwsSignerInitializer {

  /**
   * Register a store instance.
   *
   * @param bucketName the bucket name
   * @param storeConf the store configuration
   * @param dtProvider delegation token provider for the store
   * @param storeUgi ugi under which the store is operating
   */
  void registerStore(String bucketName, Configuration storeConf,
      DelegationTokenProvider dtProvider, UserGroupInformation storeUgi);

  /**
   * Unregister a store instance.
   *
   * @param bucketName the bucket name
   * @param storeConf the store configuration
   * @param dtProvider delegation token provider for the store
   * @param storeUgi ugi under which the store is operating
   */
  void unregisterStore(String bucketName, Configuration storeConf,
      DelegationTokenProvider dtProvider, UserGroupInformation storeUgi);
}
