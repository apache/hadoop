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

package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.
    DFS_ROUTER_ASYNC_RPC_MAX_ASYNCCALL_PERMIT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.
    DFS_ROUTER_ASYNC_RPC_MAX_ASYNC_CALL_PERMIT_DEFAULT;

/**
 * When router async rpc enabled, it is recommended to use this fairness controller.
 */
public class RouterAsyncRpcFairnessPolicyController extends
    AbstractRouterRpcFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAsyncRpcFairnessPolicyController.class);

  public static final String INIT_MSG = "Max async call permits per nameservice: %d";

  public RouterAsyncRpcFairnessPolicyController(Configuration conf) {
    init(conf);
  }

  public void init(Configuration conf) throws IllegalArgumentException {
    super.init(conf);

    int maxAsyncCallPermit = conf.getInt(DFS_ROUTER_ASYNC_RPC_MAX_ASYNCCALL_PERMIT_KEY,
        DFS_ROUTER_ASYNC_RPC_MAX_ASYNC_CALL_PERMIT_DEFAULT);
    if (maxAsyncCallPermit <= 0) {
      maxAsyncCallPermit = DFS_ROUTER_ASYNC_RPC_MAX_ASYNC_CALL_PERMIT_DEFAULT;
    }
    LOG.info(String.format(INIT_MSG, maxAsyncCallPermit));

    // Get all name services configured.
    Set<String> allConfiguredNS = FederationUtil.getAllConfiguredNS(conf);

    for (String nsId : allConfiguredNS) {
      LOG.info("Dedicated permits {} for ns {} ", maxAsyncCallPermit, nsId);
      insertNameServiceWithPermits(nsId, maxAsyncCallPermit);
      logAssignment(nsId, maxAsyncCallPermit);
    }
    // Avoid NPE when router async rpc disable.
    insertNameServiceWithPermits(CONCURRENT_NS, maxAsyncCallPermit);
    LOG.info("Dedicated permits {} for ns {} ", maxAsyncCallPermit, CONCURRENT_NS);
  }

  private static void logAssignment(String nsId, int count) {
    LOG.info("Assigned {} permits to nsId {} ", count, nsId);
  }

  @Override
  public boolean acquirePermit(String nsId) {
    if (nsId.equals(CONCURRENT_NS)) {
      return true;
    }
    return super.acquirePermit(nsId);
  }

  @Override
  public void releasePermit(String nsId) {
    if (nsId.equals(CONCURRENT_NS)) {
      return;
    }
    super.releasePermit(nsId);
  }
}
