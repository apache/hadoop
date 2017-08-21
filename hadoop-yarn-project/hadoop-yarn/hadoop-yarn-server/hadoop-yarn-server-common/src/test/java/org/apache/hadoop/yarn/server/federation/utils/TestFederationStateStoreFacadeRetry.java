/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.utils;

import javax.cache.integration.CacheLoaderException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreRetriableException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;

/**
 * Test class to validate FederationStateStoreFacade retry policy.
 */
public class TestFederationStateStoreFacadeRetry {

  private int maxRetries = 4;
  private Configuration conf;

  @Before
  public void setup() {
    conf = new Configuration();
    conf.setInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES, maxRetries);
  }

  /*
   * Test to validate that FederationStateStoreRetriableException is a retriable
   * exception.
   */
  @Test
  public void testFacadeRetriableException() throws Exception {
    RetryPolicy policy = FederationStateStoreFacade.createRetryPolicy(conf);
    RetryAction action = policy.shouldRetry(
        new FederationStateStoreRetriableException(""), 0, 0, false);
    // We compare only the action, since delay and the reason are random values
    // during this test
    Assert.assertEquals(RetryAction.RETRY.action, action.action);

    // After maxRetries we stop to retry
    action = policy.shouldRetry(new FederationStateStoreRetriableException(""),
        maxRetries, 0, false);
    Assert.assertEquals(RetryAction.FAIL.action, action.action);
  }

  /*
   * Test to validate that YarnException is not a retriable exception.
   */
  @Test
  public void testFacadeYarnException() throws Exception {
    RetryPolicy policy = FederationStateStoreFacade.createRetryPolicy(conf);
    RetryAction action = policy.shouldRetry(new YarnException(), 0, 0, false);
    Assert.assertEquals(RetryAction.FAIL.action, action.action);
  }

  /*
   * Test to validate that FederationStateStoreException is not a retriable
   * exception.
   */
  @Test
  public void testFacadeStateStoreException() throws Exception {
    RetryPolicy policy = FederationStateStoreFacade.createRetryPolicy(conf);
    RetryAction action = policy
        .shouldRetry(new FederationStateStoreException("Error"), 0, 0, false);
    Assert.assertEquals(RetryAction.FAIL.action, action.action);
  }

  /*
   * Test to validate that FederationStateStoreInvalidInputException is not a
   * retriable exception.
   */
  @Test
  public void testFacadeInvalidInputException() throws Exception {
    RetryPolicy policy = FederationStateStoreFacade.createRetryPolicy(conf);
    RetryAction action = policy.shouldRetry(
        new FederationStateStoreInvalidInputException(""), 0, 0, false);
    Assert.assertEquals(RetryAction.FAIL.action, action.action);
  }

  /*
   * Test to validate that CacheLoaderException is a retriable exception.
   */
  @Test
  public void testFacadeCacheRetriableException() throws Exception {
    RetryPolicy policy = FederationStateStoreFacade.createRetryPolicy(conf);
    RetryAction action =
        policy.shouldRetry(new CacheLoaderException(""), 0, 0, false);
    // We compare only the action, since delay and the reason are random values
    // during this test
    Assert.assertEquals(RetryAction.RETRY.action, action.action);

    // After maxRetries we stop to retry
    action =
        policy.shouldRetry(new CacheLoaderException(""), maxRetries, 0, false);
    Assert.assertEquals(RetryAction.FAIL.action, action.action);
  }

  /*
   * Test to validate that PoolInitializationException is a retriable exception.
   */
  @Test
  public void testFacadePoolInitRetriableException() throws Exception {
    // PoolInitializationException is a retriable exception
    RetryPolicy policy = FederationStateStoreFacade.createRetryPolicy(conf);
    RetryAction action = policy.shouldRetry(
        new PoolInitializationException(new YarnException()), 0, 0, false);
    // We compare only the action, delay and the reason are random value
    // during this test
    Assert.assertEquals(RetryAction.RETRY.action, action.action);

    // After maxRetries we stop to retry
    action =
        policy.shouldRetry(new PoolInitializationException(new YarnException()),
            maxRetries, 0, false);
    Assert.assertEquals(RetryAction.FAIL.action, action.action);
  }
}
