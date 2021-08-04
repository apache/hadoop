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

package org.apache.hadoop.fs.s3a.audit;

import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.RequestHandler2;

/**
 * Simple AWS handler to verify dynamic loading of extra request
 * handlers during auditing setup.
 * The invocation counter tracks the count of calls to
 * {@link #beforeExecution(AmazonWebServiceRequest)}.
 */
public final class SimpleAWSRequestHandler extends RequestHandler2 {

  public static final String CLASS
      = "org.apache.hadoop.fs.s3a.audit.SimpleAWSRequestHandler";

  /** Count of invocations. */
  private static final AtomicLong INVOCATIONS = new AtomicLong(0);

  @Override
  public AmazonWebServiceRequest beforeExecution(
      final AmazonWebServiceRequest request) {
    INVOCATIONS.incrementAndGet();
    return request;
  }

  /**
   * Get the count of invocations.
   * @return a natural number.
   */
  public static long getInvocationCount() {
    return INVOCATIONS.get();
  }
}
