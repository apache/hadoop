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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;

/**
 * Unit test of the input policy logic, without making any S3 calls.
 */
public class TestS3AInputPolicies {

  private S3AInputPolicy policy;
  private long targetPos;
  private long length;
  private long contentLength;
  private long readahead;
  private long expectedLimit;

  public static final long _64K = 64 * 1024;
  public static final long _128K = 128 * 1024;
  public static final long _256K = 256 * 1024;
  public static final long _1MB = 1024L * 1024;
  public static final long _10MB = _1MB * 10;

  public void initTestS3AInputPolicies(S3AInputPolicy pPolicy,
      long pTargetPos,
      long pLength,
      long pContentLength,
      long pReadahead,
      long pExpectedLimit) {
    this.policy = pPolicy;
    this.targetPos = pTargetPos;
    this.length = pLength;
    this.contentLength = pContentLength;
    this.readahead = pReadahead;
    this.expectedLimit = pExpectedLimit;
  }

  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {S3AInputPolicy.Normal, 0, -1, 0, _64K, 0},
        {S3AInputPolicy.Normal, 0, -1, _10MB, _64K, _10MB},
        {S3AInputPolicy.Normal, _64K, _64K, _10MB, _64K, _10MB},
        {S3AInputPolicy.Sequential, 0, -1, 0, _64K, 0},
        {S3AInputPolicy.Sequential, 0, -1, _10MB, _64K, _10MB},
        {S3AInputPolicy.Random, 0, -1, 0, _64K, 0},
        {S3AInputPolicy.Random, 0, -1, _10MB, _64K, _10MB},
        {S3AInputPolicy.Random, 0, _128K, _10MB, _64K, _128K},
        {S3AInputPolicy.Random, 0, _128K, _10MB, _256K, _256K},
        {S3AInputPolicy.Random, 0, 0, _10MB, _256K, _256K},
        {S3AInputPolicy.Random, 0, 1, _10MB, _256K, _256K},
        {S3AInputPolicy.Random, 0, _1MB, _10MB, _256K, _1MB},
        {S3AInputPolicy.Random, 0, _1MB, _10MB, 0, _1MB},
        {S3AInputPolicy.Random, _10MB + _64K, _1MB, _10MB, _256K, _10MB},
    });
  }

  @MethodSource("data")
  @ParameterizedTest
  public void testInputPolicies(S3AInputPolicy pPolicy,
      long pTargetPos, long pLength, long pContentLength,
      long pReadahead, long pExpectedLimit) throws Throwable {
    initTestS3AInputPolicies(pPolicy, pTargetPos, pLength, pContentLength,
        pReadahead, pExpectedLimit);
    Assertions.assertEquals(
        expectedLimit,
        S3AInputStream.calculateRequestLimit(policy, targetPos,
            length, contentLength, readahead),
        String.format("calculateRequestLimit(%s, %d, %d, %d, %d)",
            policy, targetPos, length, contentLength, readahead));
  }
}
