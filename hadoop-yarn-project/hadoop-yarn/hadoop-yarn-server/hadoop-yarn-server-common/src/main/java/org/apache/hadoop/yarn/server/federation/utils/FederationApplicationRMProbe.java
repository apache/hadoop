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

package org.apache.hadoop.yarn.server.federation.utils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the SubCluster whose RM knows an application whose home SubCluster is
 * not in the Federation StateStore, e.g. because the RM removed the completed
 * application and its home SubCluster with it.
 * <p>
 * All the RMs are asked at once, and the answers are awaited for
 * {@link YarnConfiguration#ROUTER_APPLICATION_PROBE_TIMEOUT}. The RM proxies
 * should be created with {@link #createProbeConf}, which tries each RM once
 * with short timeouts: an RM that is down but still active in the StateStore
 * would otherwise hold the lookup for yarn.resourcemanager.connect.max-wait.ms.
 * <p>
 * An RM that could not be asked, failed or did not answer in time is not
 * taken as not knowing the application, as the application may be running
 * there: {@link #find} then fails instead of returning null.
 */
public class FederationApplicationRMProbe implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationApplicationRMProbe.class);

  /** Upper bound of the RMs asked at once; the others are not asked. */
  private static final int MAX_THREADS = 64;

  /** Connect timeout towards an RM, in milliseconds. */
  private static final int CONNECT_TIMEOUT_MS = 3000;

  /** Creates the RM proxy used to ask a SubCluster. */
  @FunctionalInterface
  public interface RMProxySupplier {
    ApplicationClientProtocol get(SubClusterId subClusterId) throws Exception;
  }

  private final RMProxySupplier supplier;
  private final long timeoutMs;
  private final ThreadPoolExecutor executor;

  /**
   * @param supplier creates the RM proxies, see {@link #createProbeConf}.
   * @param timeoutMs how long to wait for the RMs to answer.
   */
  public FederationApplicationRMProbe(RMProxySupplier supplier,
      long timeoutMs) {
    this(supplier, timeoutMs, MAX_THREADS);
  }

  FederationApplicationRMProbe(RMProxySupplier supplier, long timeoutMs,
      int maxThreads) {
    this.supplier = supplier;
    this.timeoutMs = timeoutMs;
    this.executor = new ThreadPoolExecutor(0, maxThreads, 60,
        TimeUnit.SECONDS, new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("Federation application probe-%d")
            .setDaemon(true).build());
  }

  /**
   * @param conf the configuration.
   * @return the time to wait for the RMs, in milliseconds.
   */
  public static long getTimeout(Configuration conf) {
    return conf.getTimeDuration(
        YarnConfiguration.ROUTER_APPLICATION_PROBE_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_APPLICATION_PROBE_TIMEOUT,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Configuration for the RM proxies of the probe: try each RM of a
   * SubCluster once, with short connect and RPC timeouts.
   *
   * @param conf the configuration.
   * @return a copy of it for the probe.
   */
  public static Configuration createProbeConf(Configuration conf) {
    Configuration probeConf = new YarnConfiguration(conf);
    probeConf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 0);
    probeConf.setInt(YarnConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, 2);
    probeConf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100);
    probeConf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_MAX_MS, 100);
    probeConf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    probeConf.setInt(CommonConfigurationKeysPublic
        .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, 0);
    probeConf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        CONNECT_TIMEOUT_MS);
    probeConf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
        (int) Math.min(getTimeout(conf), Integer.MAX_VALUE));
    return probeConf;
  }

  /**
   * Ask the RMs of the given SubClusters for an application.
   *
   * @param appId the application.
   * @param subClusters the SubClusters to ask.
   * @return the SubCluster whose RM knows the application, or null if every
   *         RM answered that it does not.
   * @throws YarnException if no RM knows the application but some could not
   *           be asked, failed or did not answer in time.
   */
  public SubClusterId find(final ApplicationId appId,
      Collection<SubClusterId> subClusters) throws YarnException {
    CompletionService<SubClusterId> completion =
        new ExecutorCompletionService<>(executor);
    List<Future<SubClusterId>> futures = new ArrayList<>();
    int rejected = 0;
    for (final SubClusterId subClusterId : subClusters) {
      try {
        futures.add(completion.submit(() -> probe(appId, subClusterId)));
      } catch (RejectedExecutionException e) {
        rejected++;
      }
    }

    long deadline = System.currentTimeMillis() + timeoutMs;
    int answered = 0;
    int failed = 0;
    try {
      while (answered < futures.size()) {
        long remaining = deadline - System.currentTimeMillis();
        Future<SubClusterId> done =
            completion.poll(Math.max(remaining, 0), TimeUnit.MILLISECONDS);
        if (done == null) {
          break;
        }
        answered++;
        try {
          SubClusterId found = done.get();
          if (found != null) {
            return found;
          }
        } catch (ExecutionException e) {
          failed++;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      for (Future<SubClusterId> future : futures) {
        future.cancel(true);
      }
    }

    int unanswered = futures.size() - answered;
    if (rejected > 0 || failed > 0 || unanswered > 0) {
      throw new YarnException(String.format("Unable to ask every SubCluster "
          + "for %s: %d not asked, %d failed, %d did not answer within %d ms",
          appId, rejected, failed, unanswered, timeoutMs));
    }
    return null;
  }

  /**
   * @return the SubCluster if its RM knows the application, null if it
   *         answered that it does not.
   * @throws Exception if the RM could not be asked.
   */
  private SubClusterId probe(ApplicationId appId, SubClusterId subClusterId)
      throws Exception {
    try {
      ApplicationReport report = supplier.get(subClusterId)
          .getApplicationReport(GetApplicationReportRequest.newInstance(appId))
          .getApplicationReport();
      if (report != null && appId.equals(report.getApplicationId())) {
        return subClusterId;
      }
      return null;
    } catch (ApplicationNotFoundException e) {
      LOG.debug("{} is not in SubCluster {}.", appId, subClusterId);
      return null;
    } catch (Exception e) {
      LOG.warn("Unable to ask SubCluster {} for {}: {}.", subClusterId, appId,
          e.toString());
      throw e;
    }
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
