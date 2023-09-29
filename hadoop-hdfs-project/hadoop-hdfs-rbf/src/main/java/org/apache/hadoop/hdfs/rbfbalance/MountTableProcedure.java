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
package org.apache.hadoop.hdfs.rbfbalance;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedure;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Update mount table.
 * Old mount table:
 *   /a/b/c -&gt; {ns:src path:/a/b/c}
 * New mount table:
 *   /a/b/c -&gt; {ns:dst path:/a/b/c}
 */
public class MountTableProcedure extends BalanceProcedure {

  private String mount;
  private String dstPath;
  private String dstNs;
  private Configuration conf;

  public MountTableProcedure() {}

  /**
   * Update mount entry to specified dst uri.
   *
   * @param name the name of the procedure.
   * @param nextProcedure the name of the next procedure.
   * @param delayDuration the delay duration when this procedure is delayed.
   * @param mount the mount entry to be updated.
   * @param dstPath the sub-cluster uri of the dst path.
   * @param dstNs the destination sub-cluster name service id.
   * @param conf the configuration.
   */
  public MountTableProcedure(String name, String nextProcedure, long delayDuration,
      String mount, String dstPath, String dstNs, Configuration conf) {
    super(name, nextProcedure, delayDuration);
    this.mount = mount;
    this.dstPath = dstPath;
    this.dstNs = dstNs;
    this.conf = conf;
  }

  @Override
  public boolean execute() throws RetryException, IOException {
    updateMountTable();
    return true;
  }

  private void updateMountTable() throws IOException {
    updateMountTableDestination(mount, dstNs, dstPath, conf);
    enableWrite(mount, conf);
  }

  /**
   * Update the destination of the mount point to target namespace and target
   * path.
   *
   * @param mount   the mount point.
   * @param dstNs   the target namespace.
   * @param dstPath the target path
   * @param conf    the configuration of the router.
   */
  private static void updateMountTableDestination(String mount, String dstNs,
      String dstPath, Configuration conf) throws IOException {
    String address = conf.getTrimmed(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
    InetSocketAddress routerSocket = NetUtils.createSocketAddr(address);
    RouterClient rClient = new RouterClient(routerSocket, conf);
    try {
      MountTableManager mountTable = rClient.getMountTableManager();

      MountTable originalEntry = getMountEntry(mount, mountTable);
      if (originalEntry == null) {
        throw new IOException("Mount table " + mount + " doesn't exist");
      } else {
        RemoteLocation remoteLocation =
            new RemoteLocation(dstNs, dstPath, mount);
        originalEntry.setDestinations(Arrays.asList(remoteLocation));
        UpdateMountTableEntryRequest updateRequest =
            UpdateMountTableEntryRequest.newInstance(originalEntry);
        UpdateMountTableEntryResponse response =
            mountTable.updateMountTableEntry(updateRequest);
        if (!response.getStatus()) {
          throw new IOException("Failed update mount table " + mount);
        }
        rClient.getMountTableManager().refreshMountTableEntries(
            RefreshMountTableEntriesRequest.newInstance());
      }
    } finally {
      rClient.close();
    }
  }

  /**
   * Gets the mount table entry.
   * @param mount name of the mount entry.
   * @param mountTable the mount table.
   * @return corresponding mount entry.
   * @throws IOException in case of failure to retrieve mount entry.
   */
  public static MountTable getMountEntry(String mount,
      MountTableManager mountTable)
      throws IOException {
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(mount);
    GetMountTableEntriesResponse getResponse =
        mountTable.getMountTableEntries(getRequest);
    List<MountTable> results = getResponse.getEntries();
    MountTable existingEntry = null;
    for (MountTable result : results) {
      if (mount.equals(result.getSourcePath())) {
        existingEntry = result;
        break;
      }
    }
    return existingEntry;
  }

  /**
   * Disable write by making the mount point readonly.
   *
   * @param mount the mount point to set readonly.
   * @param conf  the configuration of the router.
   */
  static void disableWrite(String mount, Configuration conf)
      throws IOException {
    setMountReadOnly(mount, true, conf);
  }

  /**
   * Enable write by cancelling the mount point readonly.
   *
   * @param mount the mount point to cancel readonly.
   * @param conf  the configuration of the router.
   */
  static void enableWrite(String mount, Configuration conf) throws IOException {
    setMountReadOnly(mount, false, conf);
  }

  /**
   * Enable or disable readonly of the mount point.
   *
   * @param mount    the mount point.
   * @param readOnly enable or disable readonly.
   * @param conf     the configuration of the router.
   */
  private static void setMountReadOnly(String mount, boolean readOnly,
      Configuration conf) throws IOException {
    String address = conf.getTrimmed(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
    InetSocketAddress routerSocket = NetUtils.createSocketAddr(address);
    RouterClient rClient = new RouterClient(routerSocket, conf);
    try {
      MountTableManager mountTable = rClient.getMountTableManager();

      MountTable originalEntry = getMountEntry(mount, mountTable);
      if (originalEntry == null) {
        throw new IOException("Mount table " + mount + " doesn't exist");
      } else {
        originalEntry.setReadOnly(readOnly);
        UpdateMountTableEntryRequest updateRequest =
            UpdateMountTableEntryRequest.newInstance(originalEntry);
        UpdateMountTableEntryResponse response =
            mountTable.updateMountTableEntry(updateRequest);
        if (!response.getStatus()) {
          throw new IOException(
              "Failed update mount table " + mount + " with readonly="
                  + readOnly);
        }
        rClient.getMountTableManager().refreshMountTableEntries(
            RefreshMountTableEntriesRequest.newInstance());
      }
    } finally {
      rClient.close();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, mount);
    Text.writeString(out, dstPath);
    Text.writeString(out, dstNs);
    conf.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    mount = Text.readString(in);
    dstPath = Text.readString(in);
    dstNs = Text.readString(in);
    conf = new Configuration(false);
    conf.readFields(in);
  }

  @VisibleForTesting
  String getMount() {
    return mount;
  }

  @VisibleForTesting
  String getDstPath() {
    return dstPath;
  }

  @VisibleForTesting
  String getDstNs() {
    return dstNs;
  }
}