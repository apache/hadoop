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

import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * Simple AWS interceptor to verify dynamic loading of extra
 * execution interceptors during auditing setup.
 * The invocation counter tracks the count of calls to
 * {@link #beforeExecution}.
 */
public final class SimpleAWSExecutionInterceptor extends Configured
    implements ExecutionInterceptor {

  public static final String CLASS
      = "org.apache.hadoop.fs.s3a.audit.SimpleAWSExecutionInterceptor";

  private static Configuration staticConf;

  /** Count of invocations. */
  private static final AtomicLong INVOCATIONS = new AtomicLong(0);

  @Override
  public void beforeExecution(Context.BeforeExecution context,
      ExecutionAttributes executionAttributes) {
    INVOCATIONS.incrementAndGet();
    staticConf = getConf();
  }

  /**
   * Get the count of invocations.
   * @return a natural number.
   */
  public static long getInvocationCount() {
    return INVOCATIONS.get();
  }

  /**
   * get the static conf, which is set the config of the
   * last executor invoked.
   * @return the static configuration.
   */

  public static Configuration getStaticConf() {
    return staticConf;
  }
}
