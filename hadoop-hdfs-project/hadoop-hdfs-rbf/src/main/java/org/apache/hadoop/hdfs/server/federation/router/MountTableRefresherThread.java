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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesResponse;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for updating mount table cache on all the router.
 */
public class MountTableRefresherThread extends Thread {
  private static final Logger LOG =
      LoggerFactory.getLogger(MountTableRefresherThread.class);
  private boolean success;
  /** Admin server on which refreshed to be invoked. */
  private String adminAddress;
  private CountDownLatch countDownLatch;
  private MountTableManager manager;

  public MountTableRefresherThread(MountTableManager manager,
      String adminAddress) {
    this.manager = manager;
    this.adminAddress = adminAddress;
    setName("MountTableRefresh_" + adminAddress);
    setDaemon(true);
  }

  /**
   * Refresh mount table cache of local and remote routers. Local and remote
   * routers will be refreshed differently. Lets understand what are the
   * local and remote routers and refresh will be done differently on these
   * routers. Suppose there are three routers R1, R2 and R3. User want to add
   * new mount table entry. He will connect to only one router, not all the
   * routers. Suppose He connects to R1 and calls add mount table entry through
   * API or CLI. Now in this context R1 is local router, R2 and R3 are remote
   * routers. Because add mount table entry is invoked on R1, R1 will update the
   * cache locally it need not to make RPC call. But R1 will make RPC calls to
   * update cache on R2 and R3.
   */
  @Override
  public void run() {
    try {
      SecurityUtil.doAsLoginUser(() -> {
        if (UserGroupInformation.isSecurityEnabled()) {
          UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        }
        RefreshMountTableEntriesResponse refreshMountTableEntries = manager
            .refreshMountTableEntries(
                RefreshMountTableEntriesRequest.newInstance());
        success = refreshMountTableEntries.getResult();
        return true;
      });
    } catch (IOException e) {
      LOG.error("Failed to refresh mount table entries cache at router {}",
          adminAddress, e);
    } finally {
      countDownLatch.countDown();
    }
  }

  /**
   * @return true if cache was refreshed successfully.
   */
  public boolean isSuccess() {
    return success;
  }

  public void setCountDownLatch(CountDownLatch countDownLatch) {
    this.countDownLatch = countDownLatch;
  }

  @Override
  public String toString() {
    return "MountTableRefreshThread [success=" + success + ", adminAddress="
        + adminAddress + "]";
  }

  public String getAdminAddress() {
    return adminAddress;
  }
}
