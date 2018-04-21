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
package org.apache.hadoop.hdfs.tools.federation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.NameserviceManager;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterQuotaUsage;
import org.apache.hadoop.hdfs.server.federation.router.RouterStateManager;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnterSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnterSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides some Federation administrative access shell commands.
 */
@Private
public class RouterAdmin extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(RouterAdmin.class);

  private RouterClient client;

  public static void main(String[] argv) throws Exception {
    Configuration conf = new HdfsConfiguration();
    RouterAdmin admin = new RouterAdmin(conf);

    int res = ToolRunner.run(admin, argv);
    System.exit(res);
  }

  public RouterAdmin(Configuration conf) {
    super(conf);
  }

  /**
   * Print the usage message.
   */
  public void printUsage() {
    String usage = "Federation Admin Tools:\n"
        + "\t[-add <source> <nameservice> <destination> "
        + "[-readonly] [-order HASH|LOCAL|RANDOM|HASH_ALL] "
        + "-owner <owner> -group <group> -mode <mode>]\n"
        + "\t[-rm <source>]\n"
        + "\t[-ls <path>]\n"
        + "\t[-setQuota <path> -nsQuota <nsQuota> -ssQuota "
        + "<quota in bytes or quota size string>]\n"
        + "\t[-clrQuota <path>]\n"
        + "\t[-safemode enter | leave | get]\n"
        + "\t[-nameservice enable | disable <nameservice>]\n"
        + "\t[-getDisabledNameservices]\n";

    System.out.println(usage);
  }

  @Override
  public int run(String[] argv) throws Exception {
    if (argv.length < 1) {
      System.err.println("Not enough parameters specificed");
      printUsage();
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    // Verify that we have enough command line parameters
    if ("-add".equals(cmd)) {
      if (argv.length < 4) {
        System.err.println("Not enough parameters specificed for cmd " + cmd);
        printUsage();
        return exitCode;
      }
    } else if ("-rm".equalsIgnoreCase(cmd)) {
      if (argv.length < 2) {
        System.err.println("Not enough parameters specificed for cmd " + cmd);
        printUsage();
        return exitCode;
      }
    } else if ("-setQuota".equalsIgnoreCase(cmd)) {
      if (argv.length < 4) {
        System.err.println("Not enough parameters specificed for cmd " + cmd);
        printUsage();
        return exitCode;
      }
    } else if ("-clrQuota".equalsIgnoreCase(cmd)) {
      if (argv.length < 2) {
        System.err.println("Not enough parameters specificed for cmd " + cmd);
        printUsage();
        return exitCode;
      }
    } else if ("-safemode".equalsIgnoreCase(cmd)) {
      if (argv.length < 2) {
        System.err.println("Not enough parameters specificed for cmd " + cmd);
        printUsage();
        return exitCode;
      }
    } else if ("-nameservice".equalsIgnoreCase(cmd)) {
      if (argv.length < 3) {
        System.err.println("Not enough parameters specificed for cmd " + cmd);
        printUsage();
        return exitCode;
      }
    }

    // Initialize RouterClient
    try {
      String address = getConf().getTrimmed(
          RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
          RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
      InetSocketAddress routerSocket = NetUtils.createSocketAddr(address);
      client = new RouterClient(routerSocket, getConf());
    } catch (RPC.VersionMismatch v) {
      System.err.println(
          "Version mismatch between client and server... command aborted");
      return exitCode;
    } catch (IOException e) {
      System.err.println("Bad connection to Router... command aborted");
      return exitCode;
    }

    Exception debugException = null;
    exitCode = 0;
    try {
      if ("-add".equals(cmd)) {
        if (addMount(argv, i)) {
          System.out.println("Successfuly added mount point " + argv[i]);
        }
      } else if ("-rm".equals(cmd)) {
        if (removeMount(argv[i])) {
          System.out.println("Successfully removed mount point " + argv[i]);
        }
      } else if ("-ls".equals(cmd)) {
        if (argv.length > 1) {
          listMounts(argv[i]);
        } else {
          listMounts("/");
        }
      } else if ("-setQuota".equals(cmd)) {
        if (setQuota(argv, i)) {
          System.out.println(
              "Successfully set quota for mount point " + argv[i]);
        }
      } else if ("-clrQuota".equals(cmd)) {
        if (clrQuota(argv[i])) {
          System.out.println(
              "Successfully clear quota for mount point " + argv[i]);
        }
      } else if ("-safemode".equals(cmd)) {
        manageSafeMode(argv[i]);
      } else if ("-nameservice".equals(cmd)) {
        String subcmd = argv[i];
        String nsId = argv[i + 1];
        manageNameservice(subcmd, nsId);
      } else if ("-getDisabledNameservices".equals(cmd)) {
        getDisabledNameservices();
      } else {
        printUsage();
        return exitCode;
      }
    } catch (IllegalArgumentException arge) {
      debugException = arge;
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage();
    } catch (RemoteException e) {
      // This is a error returned by the server.
      // Print out the first line of the error message, ignore the stack trace.
      exitCode = -1;
      debugException = e;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": " + content[0]);
        e.printStackTrace();
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage());
        e.printStackTrace();
        debugException = ex;
      }
    } catch (Exception e) {
      exitCode = -1;
      debugException = e;
      System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage());
      e.printStackTrace();
    }
    if (debugException != null) {
      LOG.debug("Exception encountered", debugException);
    }
    return exitCode;
  }

  /**
   * Add a mount table entry or update if it exists.
   *
   * @param parameters Parameters for the mount point.
   * @param i Index in the parameters.
   */
  public boolean addMount(String[] parameters, int i) throws IOException {
    // Mandatory parameters
    String mount = parameters[i++];
    String[] nss = parameters[i++].split(",");
    String dest = parameters[i++];

    // Optional parameters
    boolean readOnly = false;
    String owner = null;
    String group = null;
    FsPermission mode = null;
    DestinationOrder order = DestinationOrder.HASH;
    while (i < parameters.length) {
      if (parameters[i].equals("-readonly")) {
        readOnly = true;
      } else if (parameters[i].equals("-order")) {
        i++;
        try {
          order = DestinationOrder.valueOf(parameters[i]);
        } catch(Exception e) {
          System.err.println("Cannot parse order: " + parameters[i]);
        }
      } else if (parameters[i].equals("-owner")) {
        i++;
        owner = parameters[i];
      } else if (parameters[i].equals("-group")) {
        i++;
        group = parameters[i];
      } else if (parameters[i].equals("-mode")) {
        i++;
        short modeValue = Short.parseShort(parameters[i], 8);
        mode = new FsPermission(modeValue);
      }

      i++;
    }

    return addMount(mount, nss, dest, readOnly, order,
        new ACLEntity(owner, group, mode));
  }

  /**
   * Add a mount table entry or update if it exists.
   *
   * @param mount Mount point.
   * @param nss Namespaces where this is mounted to.
   * @param dest Destination path.
   * @param readonly If the mount point is read only.
   * @param order Order of the destination locations.
   * @param aclInfo the ACL info for mount point.
   * @return If the mount point was added.
   * @throws IOException Error adding the mount point.
   */
  public boolean addMount(String mount, String[] nss, String dest,
      boolean readonly, DestinationOrder order, ACLEntity aclInfo)
      throws IOException {
    // Get the existing entry
    MountTableManager mountTable = client.getMountTableManager();
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(mount);
    GetMountTableEntriesResponse getResponse =
        mountTable.getMountTableEntries(getRequest);
    List<MountTable> results = getResponse.getEntries();
    MountTable existingEntry = null;
    for (MountTable result : results) {
      if (mount.equals(result.getSourcePath())) {
        existingEntry = result;
      }
    }

    if (existingEntry == null) {
      // Create and add the entry if it doesn't exist
      Map<String, String> destMap = new LinkedHashMap<>();
      for (String ns : nss) {
        destMap.put(ns, dest);
      }
      MountTable newEntry = MountTable.newInstance(mount, destMap);
      if (readonly) {
        newEntry.setReadOnly(true);
      }
      if (order != null) {
        newEntry.setDestOrder(order);
      }

      // Set ACL info for mount table entry
      if (aclInfo.getOwner() != null) {
        newEntry.setOwnerName(aclInfo.getOwner());
      }

      if (aclInfo.getGroup() != null) {
        newEntry.setGroupName(aclInfo.getGroup());
      }

      if (aclInfo.getMode() != null) {
        newEntry.setMode(aclInfo.getMode());
      }

      AddMountTableEntryRequest request =
          AddMountTableEntryRequest.newInstance(newEntry);
      AddMountTableEntryResponse addResponse =
          mountTable.addMountTableEntry(request);
      boolean added = addResponse.getStatus();
      if (!added) {
        System.err.println("Cannot add mount point " + mount);
      }
      return added;
    } else {
      // Update the existing entry if it exists
      for (String nsId : nss) {
        if (!existingEntry.addDestination(nsId, dest)) {
          System.err.println("Cannot add destination at " + nsId + " " + dest);
        }
      }
      if (readonly) {
        existingEntry.setReadOnly(true);
      }
      if (order != null) {
        existingEntry.setDestOrder(order);
      }

      // Update ACL info of mount table entry
      if (aclInfo.getOwner() != null) {
        existingEntry.setOwnerName(aclInfo.getOwner());
      }

      if (aclInfo.getGroup() != null) {
        existingEntry.setGroupName(aclInfo.getGroup());
      }

      if (aclInfo.getMode() != null) {
        existingEntry.setMode(aclInfo.getMode());
      }

      UpdateMountTableEntryRequest updateRequest =
          UpdateMountTableEntryRequest.newInstance(existingEntry);
      UpdateMountTableEntryResponse updateResponse =
          mountTable.updateMountTableEntry(updateRequest);
      boolean updated = updateResponse.getStatus();
      if (!updated) {
        System.err.println("Cannot update mount point " + mount);
      }
      return updated;
    }
  }

  /**
   * Remove mount point.
   *
   * @param path Path to remove.
   * @throws IOException If it cannot be removed.
   */
  public boolean removeMount(String path) throws IOException {
    MountTableManager mountTable = client.getMountTableManager();
    RemoveMountTableEntryRequest request =
        RemoveMountTableEntryRequest.newInstance(path);
    RemoveMountTableEntryResponse response =
        mountTable.removeMountTableEntry(request);
    boolean removed = response.getStatus();
    if (!removed) {
      System.out.println("Cannot remove mount point " + path);
    }
    return removed;
  }

  /**
   * List mount points.
   *
   * @param path Path to list.
   * @throws IOException If it cannot be listed.
   */
  public void listMounts(String path) throws IOException {
    MountTableManager mountTable = client.getMountTableManager();
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance(path);
    GetMountTableEntriesResponse response =
        mountTable.getMountTableEntries(request);
    List<MountTable> entries = response.getEntries();
    printMounts(entries);
  }

  private static void printMounts(List<MountTable> entries) {
    System.out.println("Mount Table Entries:");
    System.out.println(String.format(
        "%-25s %-25s %-25s %-25s %-25s %-25s",
        "Source", "Destinations", "Owner", "Group", "Mode", "Quota/Usage"));
    for (MountTable entry : entries) {
      StringBuilder destBuilder = new StringBuilder();
      for (RemoteLocation location : entry.getDestinations()) {
        if (destBuilder.length() > 0) {
          destBuilder.append(",");
        }
        destBuilder.append(String.format("%s->%s", location.getNameserviceId(),
            location.getDest()));
      }
      System.out.print(String.format("%-25s %-25s", entry.getSourcePath(),
          destBuilder.toString()));

      System.out.print(String.format(" %-25s %-25s %-25s",
          entry.getOwnerName(), entry.getGroupName(), entry.getMode()));

      System.out.println(String.format(" %-25s", entry.getQuota()));
    }
  }

  /**
   * Set quota for a mount table entry.
   *
   * @param parameters Parameters of the quota.
   * @param i Index in the parameters.
   */
  private boolean setQuota(String[] parameters, int i) throws IOException {
    long nsQuota = HdfsConstants.QUOTA_DONT_SET;
    long ssQuota = HdfsConstants.QUOTA_DONT_SET;

    String mount = parameters[i++];
    while (i < parameters.length) {
      if (parameters[i].equals("-nsQuota")) {
        i++;
        try {
          nsQuota = Long.parseLong(parameters[i]);
        } catch (Exception e) {
          throw new IllegalArgumentException(
              "Cannot parse nsQuota: " + parameters[i]);
        }
      } else if (parameters[i].equals("-ssQuota")) {
        i++;
        try {
          ssQuota = StringUtils.TraditionalBinaryPrefix
              .string2long(parameters[i]);
        } catch (Exception e) {
          throw new IllegalArgumentException(
              "Cannot parse ssQuota: " + parameters[i]);
        }
      }

      i++;
    }

    if (nsQuota <= 0 || ssQuota <= 0) {
      throw new IllegalArgumentException(
          "Input quota value should be a positive number.");
    }

    if (nsQuota == HdfsConstants.QUOTA_DONT_SET &&
        ssQuota == HdfsConstants.QUOTA_DONT_SET) {
      throw new IllegalArgumentException(
          "Must specify at least one of -nsQuota and -ssQuota.");
    }

    return updateQuota(mount, nsQuota, ssQuota);
  }

  /**
   * Clear quota of the mount point.
   *
   * @param mount Mount table to clear
   * @return If the quota was cleared.
   * @throws IOException Error clearing the mount point.
   */
  private boolean clrQuota(String mount) throws IOException {
    return updateQuota(mount, HdfsConstants.QUOTA_DONT_SET,
        HdfsConstants.QUOTA_DONT_SET);
  }

  /**
   * Update quota of specified mount table.
   *
   * @param mount Specified mount table to update.
   * @param nsQuota Namespace quota.
   * @param ssQuota Storage space quota.
   * @return If the quota was updated.
   * @throws IOException Error updating quota.
   */
  private boolean updateQuota(String mount, long nsQuota, long ssQuota)
      throws IOException {
    // Get existing entry
    MountTableManager mountTable = client.getMountTableManager();
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest
        .newInstance(mount);
    GetMountTableEntriesResponse getResponse = mountTable
        .getMountTableEntries(getRequest);
    List<MountTable> results = getResponse.getEntries();
    MountTable existingEntry = null;
    for (MountTable result : results) {
      if (mount.equals(result.getSourcePath())) {
        existingEntry = result;
        break;
      }
    }

    if (existingEntry == null) {
      throw new IOException(mount + " doesn't exist in mount table.");
    } else {
      long nsCount = existingEntry.getQuota().getFileAndDirectoryCount();
      long ssCount = existingEntry.getQuota().getSpaceConsumed();
      // If nsQuota and ssQuota were unset, clear nsQuota and ssQuota.
      if (nsQuota == HdfsConstants.QUOTA_DONT_SET &&
          ssQuota == HdfsConstants.QUOTA_DONT_SET) {
        nsCount = RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT;
        ssCount = RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT;
      } else {
        // If nsQuota or ssQuota was unset, use the value in mount table.
        if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
          nsQuota = existingEntry.getQuota().getQuota();
        }
        if (ssQuota == HdfsConstants.QUOTA_DONT_SET) {
          ssQuota = existingEntry.getQuota().getSpaceQuota();
        }
      }

      RouterQuotaUsage updatedQuota = new RouterQuotaUsage.Builder()
          .fileAndDirectoryCount(nsCount).quota(nsQuota)
          .spaceConsumed(ssCount).spaceQuota(ssQuota).build();
      existingEntry.setQuota(updatedQuota);
    }

    UpdateMountTableEntryRequest updateRequest =
        UpdateMountTableEntryRequest.newInstance(existingEntry);
    UpdateMountTableEntryResponse updateResponse = mountTable
        .updateMountTableEntry(updateRequest);
    return updateResponse.getStatus();
  }

  /**
   * Manager the safe mode state.
   * @param cmd Input command, enter or leave safe mode.
   * @throws IOException
   */
  private void manageSafeMode(String cmd) throws IOException {
    if (cmd.equals("enter")) {
      if (enterSafeMode()) {
        System.out.println("Successfully enter safe mode.");
      }
    } else if (cmd.equals("leave")) {
      if (leaveSafeMode()) {
        System.out.println("Successfully leave safe mode.");
      }
    } else if (cmd.equals("get")) {
      boolean result = getSafeMode();
      System.out.println("Safe Mode: " + result);
    }
  }

  /**
   * Request the Router entering safemode state.
   * @return Return true if entering safemode successfully.
   * @throws IOException
   */
  private boolean enterSafeMode() throws IOException {
    RouterStateManager stateManager = client.getRouterStateManager();
    EnterSafeModeResponse response = stateManager.enterSafeMode(
        EnterSafeModeRequest.newInstance());
    return response.getStatus();
  }

  /**
   * Request the Router leaving safemode state.
   * @return Return true if leaving safemode successfully.
   * @throws IOException
   */
  private boolean leaveSafeMode() throws IOException {
    RouterStateManager stateManager = client.getRouterStateManager();
    LeaveSafeModeResponse response = stateManager.leaveSafeMode(
        LeaveSafeModeRequest.newInstance());
    return response.getStatus();
  }

  /**
   * Verify if current Router state is safe mode state.
   * @return True if the Router is in safe mode.
   * @throws IOException
   */
  private boolean getSafeMode() throws IOException {
    RouterStateManager stateManager = client.getRouterStateManager();
    GetSafeModeResponse response = stateManager.getSafeMode(
        GetSafeModeRequest.newInstance());
    return response.isInSafeMode();
  }

  /**
   * Manage the name service: enabling/disabling.
   * @param cmd Input command, disable or enable.
   * @throws IOException
   */
  private void manageNameservice(String cmd, String nsId) throws IOException {
    if (cmd.equals("enable")) {
      if (enableNameservice(nsId)) {
        System.out.println("Successfully enabled nameservice " + nsId);
      } else {
        System.err.println("Cannot enable " + nsId);
      }
    } else if (cmd.equals("disable")) {
      if (disableNameservice(nsId)) {
        System.out.println("Successfully disabled nameservice " + nsId);
      } else {
        System.err.println("Cannot disable " + nsId);
      }
    } else {
      throw new IllegalArgumentException("Unknown command: " + cmd);
    }
  }

  private boolean disableNameservice(String nsId) throws IOException {
    NameserviceManager nameserviceManager = client.getNameserviceManager();
    DisableNameserviceResponse response =
        nameserviceManager.disableNameservice(
            DisableNameserviceRequest.newInstance(nsId));
    return response.getStatus();
  }

  private boolean enableNameservice(String nsId) throws IOException {
    NameserviceManager nameserviceManager = client.getNameserviceManager();
    EnableNameserviceResponse response =
        nameserviceManager.enableNameservice(
            EnableNameserviceRequest.newInstance(nsId));
    return response.getStatus();
  }

  private void getDisabledNameservices() throws IOException {
    NameserviceManager nameserviceManager = client.getNameserviceManager();
    GetDisabledNameservicesRequest request =
        GetDisabledNameservicesRequest.newInstance();
    GetDisabledNameservicesResponse response =
        nameserviceManager.getDisabledNameservices(request);
    System.out.println("List of disabled nameservices:");
    for (String nsId : response.getNameservices()) {
      System.out.println(nsId);
    }
  }

  /**
   * Inner class that stores ACL info of mount table.
   */
  static class ACLEntity {
    private final String owner;
    private final String group;
    private final FsPermission mode;

    ACLEntity(String owner, String group, FsPermission mode) {
      this.owner = owner;
      this.group = group;
      this.mode = mode;
    }

    public String getOwner() {
      return owner;
    }

    public String getGroup() {
      return group;
    }

    public FsPermission getMode() {
      return mode;
    }
  }
}