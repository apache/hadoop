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

import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * A 5xx response came back from a service.
 * The 500 error considered retriable by the AWS SDK, which will have already
 * tried it {@code fs.s3a.attempts.maximum} times before reaching s3a
 * code.
 * How it handles other 5xx errors is unknown: S3A FS code will treat them
 * as unrecoverable on the basis that they indicate some third-party store
 * or gateway problem.
 */
public class AWSStatus500Exception extends AWSServiceIOException {
  public AWSStatus500Exception(String operation,
      AwsServiceException cause) {
    super(operation, cause);
  }

  @Override
  public boolean retryable() {
    return false;
  }
}
