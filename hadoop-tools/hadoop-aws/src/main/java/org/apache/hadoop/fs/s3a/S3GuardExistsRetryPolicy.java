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

import java.io.FileNotFoundException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;


/**
 * Slightly-modified retry policy for cases when the file is present in the
 * MetadataStore, but may be still throwing FileNotFoundException from S3.
 */
public class S3GuardExistsRetryPolicy extends S3ARetryPolicy {
  /**
   * Instantiate.
   * @param conf configuration to read.
   */
  public S3GuardExistsRetryPolicy(Configuration conf) {
    super(conf);
  }

  @Override
  protected Map<Class<? extends Exception>, RetryPolicy> createExceptionMap() {
    Map<Class<? extends Exception>, RetryPolicy> b = super.createExceptionMap();
    b.put(FileNotFoundException.class, retryIdempotentCalls);
    return b;
  }
}
