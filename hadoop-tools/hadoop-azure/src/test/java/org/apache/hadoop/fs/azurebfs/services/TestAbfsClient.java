/**
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

package org.apache.hadoop.fs.azurebfs.services;

import java.net.URI;
import java.net.URL;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbfsCountersImpl;
import org.apache.hadoop.fs.azurebfs.MockIntercept;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.MetricFormat;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_ACCOUNT_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_FORMAT;
import static org.apache.hadoop.fs.azurebfs.services.AbfsClient.ABFS_CLIENT_TIMER_THREAD_NAME;
import static org.mockito.ArgumentMatchers.any;

/**
 * Unit test cases for the AbfsClient class.
 */
public class TestAbfsClient {
    private static final String ACCOUNT_NAME = "bogusAccountName.dfs.core.windows.net";
    private static final String ACCOUNT_KEY = "testKey";
    private static final long SLEEP_DURATION_MS = 500;

    /**
     * Test the initialization of the AbfsClient timer when metric collection is disabled.
     * In case of metric collection being disabled, the timer should not be initialized.
     * Asserting that the timer is null and the abfs-timer-client thread is not running.
     */
    @Test
    public void testTimerInitializationWithoutMetricCollection() throws Exception {
        final Configuration configuration = new Configuration();
        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration, ACCOUNT_NAME);

        AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
        AbfsClientContext abfsClientContext = new AbfsClientContextBuilder().withAbfsCounters(abfsCounters).build();

        // Get an instance of AbfsClient.
        AbfsClient client = new AbfsDfsClient(new URL("https://azure.com"),
                null,
                abfsConfiguration,
                (AccessTokenProvider) null,
                null,
                abfsClientContext);

        Assertions.assertThat(client.getTimer())
                .describedAs("Timer should not be initialized")
                .isNull();

        // Check if a thread with the name "abfs-timer-client" exists
        Assertions.assertThat(isThreadRunning(ABFS_CLIENT_TIMER_THREAD_NAME))
                .describedAs("Expected thread 'abfs-timer-client' not found")
                .isEqualTo(false);
        client.close();
    }

    /**
     * Test the initialization of the AbfsClient timer when metric collection is enabled.
     * In case of metric collection being enabled, the timer should be initialized.
     * Asserting that the timer is not null and the abfs-timer-client thread is running.
     * Also, asserting that the thread is removed after closing the client.
     */
    @Test
    public void testTimerInitializationWithMetricCollection() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(FS_AZURE_METRIC_FORMAT, String.valueOf(MetricFormat.INTERNAL_BACKOFF_METRIC_FORMAT));
        configuration.set(FS_AZURE_METRIC_ACCOUNT_NAME, ACCOUNT_NAME);
        configuration.set(FS_AZURE_METRIC_ACCOUNT_KEY, Base64.encode(ACCOUNT_KEY.getBytes()));
        AbfsConfiguration abfsConfiguration = new AbfsConfiguration(configuration, ACCOUNT_NAME);

        AbfsCounters abfsCounters = Mockito.spy(new AbfsCountersImpl(new URI("abcd")));
        AbfsClientContext abfsClientContext = new AbfsClientContextBuilder().withAbfsCounters(abfsCounters).build();

        // Get an instance of AbfsClient.
        AbfsClient client = new AbfsDfsClient(new URL("https://azure.com"),
                null,
                abfsConfiguration,
                (AccessTokenProvider) null,
                null,
                abfsClientContext);

        Assertions.assertThat(client.getTimer())
                .describedAs("Timer should be initialized")
                .isNotNull();

        // Check if a thread with the name "abfs-timer-client" exists
        Assertions.assertThat(isThreadRunning(ABFS_CLIENT_TIMER_THREAD_NAME))
                .describedAs("Expected thread 'abfs-timer-client' not found")
                .isEqualTo(true);
        client.close();

        // Check if the thread is removed after closing the client
        Thread.sleep(SLEEP_DURATION_MS);
        Assertions.assertThat(isThreadRunning(ABFS_CLIENT_TIMER_THREAD_NAME))
                .describedAs("Unexpected thread 'abfs-timer-client' found")
                .isEqualTo(false);
    }

    /**
     * Check if a thread with the specified name is running.
     *
     * @param threadName Name of the thread to check
     * @return true if the thread is running, false otherwise
     */
    private boolean isThreadRunning(String threadName) {
        // Get all threads and their stack traces
        Map<Thread, StackTraceElement[]> allThreads = Thread.getAllStackTraces();

        // Check if any thread has the specified name
        for (Thread thread : allThreads.keySet()) {
            if (thread.getName().equals(threadName)) {
                return true;
            }
        }
        return false;
    }

  /**
   * Mocks the creation of an `AbfsRestOperation` for the given `AbfsClient` and intercepts its execution.
   * This method sets up a mock behavior where the `AbfsRestOperation` will call the provided `MockIntercept`
   * to handle custom logic during the operation execution.
   *
   * @param abfsClient the `AbfsClient` to mock the operation for
   * @param mockIntercept the mock interceptor that defines custom behavior during the operation execution
   * @throws Exception if an error occurs while mocking the operation creation
   */
  public static void mockAbfsOperationCreation(final AbfsClient abfsClient,
      final MockIntercept mockIntercept) throws Exception {
    boolean[] flag = new boolean[1];
    Mockito.doAnswer(answer -> {
          if (!flag[0]) {
            flag[0] = true;
            AbfsRestOperation op = Mockito.spy(
                new AbfsRestOperation(
                    answer.getArgument(0),
                    abfsClient,
                    answer.getArgument(1),
                    answer.getArgument(2),
                    answer.getArgument(3),
                    abfsClient.getAbfsConfiguration()
                ));
            Mockito.doAnswer((answer1) -> {
                  mockIntercept.answer(op, answer1);
                  return null;
                }).when(op)
                .execute(any());
            Mockito.doReturn(true).when(op).isARetriedRequest();
            return op;
          }
          return answer.callRealMethod();
        }).when(abfsClient)
        .getAbfsRestOperation(any(), any(), any(), any());
  }
}
