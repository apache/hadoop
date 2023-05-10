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
package org.apache.hadoop.hdfs.tools.federation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.RouterGenericManager;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.NameserviceManager;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterQuotaUsage;
import org.apache.hadoop.hdfs.server.federation.router.RouterStateManager;
import org.apache.hadoop.hdfs.server.federation.store.CachedRecordStore;
import org.apache.hadoop.hdfs.server.federation.store.RecordStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnterSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;
import org.apache.hadoop.hdfs.tools.AdminHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolClientSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolClientSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hdfs.server.federation.router.Quota.eachByStorageType;
import static org.apache.hadoop.hdfs.server.federation.router.Quota.orByStorageType;
import static org.apache.hadoop.hdfs.server.federation.router.Quota.andByStorageType;
import static org.apache.hadoop.hdfs.tools.AdminHelper.detailHelp;

/**
 * This class provides some Federation administrative access shell commands.
 */
@Private
public class RouterAdmin extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(RouterAdmin.class);

  /** Pre-compiled regular expressions to detect duplicated slashes. */
  private static final Pattern SLASHES = Pattern.compile("/+");

  public static void main(String[] argv) throws Exception {
    Configuration conf = new HdfsConfiguration();
    RouterAdmin admin = new RouterAdmin(conf);

    System.exit(ToolRunner.run(admin, argv));
  }

  public RouterAdmin(Configuration conf) {
    super(conf);
  }

  /**
   * Get the configured address for the router admin port.
   * @param conf the configuration to check
   * @return the address
   */
  private static String getAddress(Configuration conf) {
    return conf.getTrimmed(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
  }

  private static RouterClient createClient(Configuration conf)
      throws IOException {
    try {
      String address = getAddress(conf);
      InetSocketAddress routerSocket = NetUtils.createSocketAddr(address);
      return new RouterClient(routerSocket, conf);
    } catch (RPC.VersionMismatch v) {
      throw new IOException(
          "Version mismatch between client and server... command aborted", v);
    } catch (IOException e) {
      throw new IOException("Bad connection to Router... command aborted", e);
    }
  }

  @Override
  public int run(String[] argv) throws Exception {
    Exception debugException = null;
    int exitCode;
    String cmd = argv.length > 0 ? argv[0].substring(1) : "?";
    try {
      return AdminHelper.runCommand("dfsrouteradmin", getConf(), argv,
          AdminHelper.makeCommand("-add",
              "Add or update a mount point.",
              "[-readonly] [-faulttolerant] " +
                  "[-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] " +
                  "[-owner <owner>] [-group <group>] [-mode <mode>] " +
                  "<source> <nameservices> <destination>",
              this::addMount,
              detailHelp("-readonly", "The mount point is read-only"),
              detailHelp("-faulttolerant", "Should the mount point tolerate failures."),
              detailHelp("-owner <owner>", "The owner of the mount point"),
              detailHelp("-group <group>", "The group of the mount point"),
              detailHelp("-mode <mode>", "The octal permissions of the mount point"),
              detailHelp("<source>", "The virtual path"),
              detailHelp("<nameservices>", "The comma separated list of target name services"),
              detailHelp("<destination>", "The target path")
          ),
          AdminHelper.makeCommand("-clrQuota",
              "Clear the quota on a mount point",
              "<path>*",
              this::clrQuota,
              detailHelp("<path>", "Mount point to clear the quota")),
          AdminHelper.makeCommand("-clrStorageTypeQuota",
              "Clear the quota on a mount point for a storage type",
              "<path>*",
              this::clrStorageTypeQuota,
              detailHelp("<path>", "Mount point to clear the quota")),
          AdminHelper.makeCommand("-dumpState",
              "Dump the routers' StateStore",
              "",
              RouterAdmin::dumpStateStore),
          AdminHelper.makeCommand("-getDestination",
              "Get the destination for a mount point",
              "[<source>]",
              this::getDestination,
              detailHelp("<source>", "The virtual path")),
          AdminHelper.makeCommand("-getDisabledNameservices",
              "List the disabled name services",
              "",
              this::getDisabledNameservices),
          AdminHelper.makeCommand("-ls",
              "List mount points",
              "[<source>] [-d]",
              this::listMounts,
              detailHelp("<source>", "The virtual path or '/'"),
              detailHelp("d", "Display detailed information")),
          AdminHelper.makeCommand("-nameservice",
              "Change the state of a name service",
              "(enable | disable) <nameservice>",
              this::manageNameservice,
              detailHelp("enable | disable", "Whether to enable or disable"),
              detailHelp("<nameservice>", "The name service to modify")),
          AdminHelper.makeCommand("-refresh",
              "Refresh router caches",
              "",
              this::refresh),
          AdminHelper.makeCommand("-refreshCallQueue",
              "Refresh call queue",
              "",
              this::refreshCallQueue),
          AdminHelper.makeCommand("-refreshRouterArgs",
              "Generic refresh via the RefreshRegistry",
              "<host:rpc_port> <key> [<arg>...]]",
              this::genericRefresh,
              detailHelp("<host:rpc_port>", "Router name and rpc port"),
              detailHelp("<key>", "the refresher's key (eg. RefreshFairnessPolicyController)"),
              detailHelp("<arg>", "The arguments to the refresher")),
          AdminHelper.makeCommand("-refreshSuperUserGroupsConfiguration",
              "Refresh super user configuration",
              "",
              this::refreshSuperUserGroupsConfiguration),
          AdminHelper.makeCommand("-rm",
              "Remove a mount point",
              "<source>*",
              this::removeMount),
          AdminHelper.makeCommand("-safemode",
              "Change the safemode state",
              "enter | leave | get",
              this::manageSafeMode),
          AdminHelper.makeCommand("-setQuota",
              "Set a quota on a mount point",
              "-nsQuota <objectQuota> -ssQuota <byteQuota> <path>",
              this::setQuota,
              detailHelp("<objectQuota>", "Maximum number of files & directories"),
              detailHelp("<byteQuota>", "Maximum number of bytes"),
              detailHelp("<path>", "The mount point to set quota")),
          AdminHelper.makeCommand("-setStorageTypeQuota",
              "Set storage type quota for a mount table entry.",
              "<path> <quota> -storageType <storageType>",
              this::setStorageTypeQuota,
              detailHelp("<path>", "The mount point where to set quota"),
              detailHelp("<quota>", "Maximum number of bytes"),
              detailHelp("<storageType>", "The type of storage (eg. disk)")),
          AdminHelper.makeCommand("-update",
              "Update a mount point.",
                  "[-readonly (true | false)] " +
                      "[-faulttolerant (true | false)] " +
                      "[-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] " +
                      "[-owner <owner>] [-group <group>] [-mode <mode>] " +
                      "<source> [<nameservices> <destination>]",
              this::updateMount,
              detailHelp("-readonly", "The mount point is read-only"),
              detailHelp("-faulttolerant", "Should the mount point tolerate failures."),
              detailHelp("<owner>", "The owner of the mount point"),
              detailHelp("<group>", "The group of the mount point"),
              detailHelp("<mode>", "The octal permissions of the mount point"),
              detailHelp("<source>", "The virtual path"),
              detailHelp("<nameservices>", "The comma separated list of target name services"),
              detailHelp("<destination>", "The target path")
          )
      );
    } catch (RemoteException e) {
      // This is an error returned by the server.
      // Print out the first line of the error message, ignore the stack trace.
      exitCode = -1;
      debugException = e;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd+ ": " + content[0]);
        e.printStackTrace();
      } catch (Exception ex) {
        System.err.println(cmd + ": " + ex.getLocalizedMessage());
        e.printStackTrace();
        debugException = ex;
      }
    } catch (IOException ioe) {
      exitCode = -1;
      System.err.printf("ERROR: %s: %s", cmd, ioe.getLocalizedMessage());
    } catch (Exception e) {
      exitCode = -1;
      debugException = e;
      System.err.printf("ERROR: %s: %s", cmd, e.getLocalizedMessage());
      e.printStackTrace();
    }
    if (LOG.isDebugEnabled() && debugException != null) {
      LOG.debug("Exception encountered", debugException);
    }
    return exitCode;
  }

  /**
   * Refresh superuser proxy groups mappings on Router.
   *
   * @throws IOException if the operation was not successful.
   */
  private int refreshSuperUserGroupsConfiguration(Configuration conf,
                                                  List<String> args)
      throws IOException{
    StringUtils.ensureAllUsed(args);
    try (RouterClient client = createClient(conf)) {
      RouterGenericManager proxy = client.getRouterGenericManager();
      String address = getAddress(conf);
      if (proxy.refreshSuperUserGroupsConfiguration()) {
        System.out.println(
            "Successfully updated superuser proxy groups on router " + address);
        return 0;
      }
    }
    return -1;
  }

  private int refresh(Configuration conf, List<String> args) throws IOException {
    StringUtils.ensureAllUsed(args);
    if (refreshRouterCache(conf)) {
      System.out.println(
          "Successfully updated mount table cache on router " + getAddress(conf));
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Refresh mount table cache on connected router.
   * @param conf The configuration to use
   * @return true if cache refreshed successfully
   */
  private boolean refreshRouterCache(Configuration conf) throws IOException {
    try (RouterClient client = createClient(conf)) {
      RefreshMountTableEntriesResponse response =
          client.getMountTableManager().refreshMountTableEntries(
              RefreshMountTableEntriesRequest.newInstance());
      return response.getResult();
    }
  }

  /**
   * Add a mount table entry or update if it exists.
   * @param conf Configuration
   * @param parameters Parameters for the mount point.
   * @return 0 if it was successful.
   * @throws IOException If it cannot add the mount point.
   */
  public int addMount(Configuration conf,
                      List<String> parameters) throws IOException {
    boolean readOnly = StringUtils.popOption("-readonly", parameters);
    boolean faultTolerant = StringUtils.popOption("-faulttolerant", parameters);
    String owner = StringUtils.popOptionWithArgument("-owner", parameters);
    String group = StringUtils.popOptionWithArgument("-group", parameters);
    String modeStr = StringUtils.popOptionWithArgument("-mode", parameters);
    DestinationOrder order = DestinationOrder.valueOf(
        StringUtils.popOptionWithArgument("-order", parameters, "HASH"));
    String mount = StringUtils.popFirstNonOption(parameters);
    String nssString = StringUtils.popFirstNonOption(parameters);
    String dest = StringUtils.popFirstNonOption(parameters);
    if (mount == null || nssString == null || dest == null) {
      throw new IllegalArgumentException("Required parameters not supplied.");
    }
    StringUtils.ensureAllUsed(parameters);
    String[] nss = nssString.split(",");

    FsPermission mode = modeStr == null ?
        null : new FsPermission(Short.parseShort(modeStr, 8));

    return addMount(conf, mount, nss, dest, readOnly, faultTolerant, order,
        new ACLEntity(owner, group, mode), true);
  }

  /**
   * Check for an optional boolean argument.
   * @param name the name of the argument
   * @param parameters the list of all of the parameters
   * @return null if the argument wasn't present, true or false based on the
   * string
   */
  static Boolean parseOptionalBoolean(String name, List<String> parameters) {
    String val = StringUtils.popOptionWithArgument(name, parameters);
    if (val == null) {
      return null;
    } else {
      switch (val.toLowerCase()) {
        case "true": return true;
        case "false": return false;
        default: throw new IllegalArgumentException("Illegal boolean value " + val);
      }
    }
  }

  /**
   * Update a mount table entry
   * @param conf Configuration
   * @param parameters Parameters for the mount point.
   * @return 0 if it was successful.
   * @throws IOException If it cannot add the mount point.
   */
  public int updateMount(Configuration conf,
                         List<String> parameters) throws IOException {
    Boolean readOnly = parseOptionalBoolean("-readonly", parameters);
    Boolean faultTolerant = parseOptionalBoolean("-faulttolerant", parameters);
    String owner = StringUtils.popOptionWithArgument("-owner", parameters);
    String group = StringUtils.popOptionWithArgument("-group", parameters);
    String modeStr = StringUtils.popOptionWithArgument("-mode", parameters);
    String orderStr = StringUtils.popOptionWithArgument("-order", parameters);
    DestinationOrder order =
        orderStr == null ? null : DestinationOrder.valueOf(orderStr);
    String mount = StringUtils.popFirstNonOption(parameters);
    String nssString = StringUtils.popFirstNonOption(parameters);
    String dest = StringUtils.popFirstNonOption(parameters);
    if (mount == null) {
      throw new IllegalArgumentException("Required mount parameter not supplied.");
    }
    if ((nssString == null) != (dest == null)) {
      throw new IllegalArgumentException(
          "Namespace and destination parameter should be supplied together.");
    }
    StringUtils.ensureAllUsed(parameters);
    String[] nss = nssString == null ? null : nssString.split(",");

    FsPermission mode = modeStr == null ?
        null : new FsPermission(Short.parseShort(modeStr, 8));

    return addMount(conf, mount, nss, dest, readOnly, faultTolerant, order,
        new ACLEntity(owner, group, mode), false);
  }

  /**
   * Add a mount table entry or update if it exists.
   * @param conf The configuration
   * @param mount Mount point.
   * @param nss Namespaces where this is mounted to.
   * @param dest Destination path.
   * @param readonly If the mount point is read only.
   * @param order Order of the destination locations.
   * @param aclInfo the ACL info for mount point.
   * @param create should the mount be created if it doesn't exist yet?
   * @return 0 if the mount point was added.
   * @throws IOException Error adding the mount point.
   */
  public int addMount(Configuration conf,
                      String mount, String[] nss, String dest,
                      Boolean readonly, Boolean faultTolerant, DestinationOrder order,
                      ACLEntity aclInfo, boolean create) throws IOException {
    mount = normalizeFileSystemPath(mount);
    try (RouterClient client = createClient(conf)) {
      // Get the existing entry
      MountTableManager mountTable = client.getMountTableManager();
      MountTable existingEntry = getMountEntry(mount, mountTable);

      if (existingEntry == null) {
        if (!create) {
          System.err.println("Mount point " + mount + " does not exist, use -add.");
          return -1;
        }
        // Create and add the entry if it doesn't exist
        Map<String, String> destMap = new LinkedHashMap<>();
        for (String ns : nss) {
          destMap.put(ns, dest);
        }
        MountTable newEntry = MountTable.newInstance(mount, destMap);
        if (readonly != null) {
          newEntry.setReadOnly(readonly);
        }
        if (faultTolerant != null) {
          newEntry.setFaultTolerant(faultTolerant);
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

        newEntry.validate();

        AddMountTableEntryRequest request =
            AddMountTableEntryRequest.newInstance(newEntry);
        AddMountTableEntryResponse addResponse =
            mountTable.addMountTableEntry(request);
        boolean added = addResponse.getStatus();
        if (!added) {
          System.err.println("Cannot add mount point " + mount);
        } else {
          System.out.println("Successfully added mount point " + mount);
        }
        return added ? 0 : -1;
      } else {
        // Update the destinations
        if (nss != null && dest != null) {
          if (create) {
            // If they are using -add, we add new destinations
            for (String nsId : nss) {
              if (!existingEntry.addDestination(nsId, dest)) {
                System.err.println("Cannot add destination at " + nsId + " " + dest);
                return -1;
              }
            }
          } else {
            // On -update we replace the old destinations
            List<RemoteLocation> locations = new ArrayList<>(nss.length);
            String path = normalizeFileSystemPath(dest);
            for (String nsId: nss) {
              locations.add(new RemoteLocation(nsId, path, mount));
            }
            existingEntry.setDestinations(locations);
          }
        }
        if (readonly != null) {
          existingEntry.setReadOnly(readonly);
        }
        if (faultTolerant != null) {
          existingEntry.setFaultTolerant(faultTolerant);
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

        existingEntry.validate();

        UpdateMountTableEntryRequest updateRequest =
            UpdateMountTableEntryRequest.newInstance(existingEntry);
        UpdateMountTableEntryResponse updateResponse =
            mountTable.updateMountTableEntry(updateRequest);
        boolean updated = updateResponse.getStatus();
        if (!updated) {
          System.err.println("Cannot update mount point " + mount);
        } else {
          System.out.println("Successfully updated mount point " + mount);
        }
        return updated ? 0 : -1;
      }
    }
  }

  /**
   * Gets the mount table entry.
   * @param mount name of the mount entry.
   * @param mountTable the mount table.
   * @return corresponding mount entry.
   * @throws IOException in case of failure to retrieve mount entry.
   */
  private MountTable getMountEntry(String mount, MountTableManager mountTable)
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
      }
    }
    return existingEntry;
  }

  /**
   * Remove mount point.
   * @param conf Configuration
   * @param args Command arguements
   * @return 0 if the mount point was removed successfully.
   * @throws IOException If it cannot be removed.
   */
  public int removeMount(Configuration conf, List<String> args) throws IOException {
    String path = StringUtils.popFirstNonOption(args);
    if (path == null) {
      throw new IllegalArgumentException("Missing parameter for remove mount");
    }
    int result = 0;
    try (RouterClient client = createClient(conf)) {
      while (path != null) {
        path = normalizeFileSystemPath(path);
        MountTableManager mountTable = client.getMountTableManager();
        RemoveMountTableEntryRequest request =
            RemoveMountTableEntryRequest.newInstance(path);
        RemoveMountTableEntryResponse response =
            mountTable.removeMountTableEntry(request);
        if (!response.getStatus()) {
          System.err.println("Cannot remove mount point " + path);
          result = -1;
        }
        path = StringUtils.popFirstNonOption(args);
      }
      return result;
    }
  }

  /**
   * List mount points.
   * @param conf Configuration to use
   * @param args Parameters of the path.
   * @return 0 if the command is successful
   * @throws IOException If it cannot be listed.
   */
  public int listMounts(Configuration conf, List<String> args) throws IOException {
    boolean detail = StringUtils.popOption("-d", args);
    String path = StringUtils.popFirstNonOption(args);
    if (path == null) {
      path = "/";
    }
    StringUtils.ensureAllUsed(args);

    try (RouterClient client = createClient(conf)) {
      path = normalizeFileSystemPath(path);
      MountTableManager mountTable = client.getMountTableManager();
      GetMountTableEntriesRequest request =
          GetMountTableEntriesRequest.newInstance(path);
      GetMountTableEntriesResponse response =
          mountTable.getMountTableEntries(request);
      List<MountTable> entries = response.getEntries();
      printMounts(entries, detail);
    }
    return 0;
  }

  private static void printMounts(List<MountTable> entries, boolean detail) {
    System.out.println("Mount Table Entries:");
    if (detail) {
      System.out.printf("%-25s %-25s %-25s %-25s %-10s %-30s %-10s %-10s %-15s%n",
          "Source", "Destinations", "Owner", "Group", "Mode", "Quota/Usage",
          "Order", "ReadOnly", "FaultTolerant");
    } else {
      System.out.printf("%-25s %-25s %-25s %-25s %-10s %-30s%n",
          "Source", "Destinations", "Owner", "Group", "Mode", "Quota/Usage");
    }
    for (MountTable entry : entries) {
      StringBuilder destBuilder = new StringBuilder();
      for (RemoteLocation location : entry.getDestinations()) {
        if (destBuilder.length() > 0) {
          destBuilder.append(",");
        }
        destBuilder.append(String.format("%s->%s", location.getNameserviceId(),
            location.getDest()));
      }
      System.out.printf("%-25s %-25s", entry.getSourcePath(), destBuilder);

      System.out.printf(" %-25s %-25s %-10s",
          entry.getOwnerName(), entry.getGroupName(), entry.getMode());

      System.out.printf(" %-30s", entry.getQuota());

      if (detail) {
        System.out.printf(" %-10s", entry.getDestOrder());

        System.out.printf(" %-10s", entry.isReadOnly() ? "Read-Only" : "");

        System.out.printf(" %-15s",
            entry.isFaultTolerant() ? "Fault-Tolerant" : "");
      }
      System.out.println();
    }
  }

  private int getDestination(Configuration conf,
                             List<String> args) throws IOException {
    String path = StringUtils.popFirstNonOption(args);
    if (path == null) {
      throw new IllegalArgumentException("Missing source parameter");
    }
    StringUtils.ensureAllUsed(args);
    try (RouterClient client = createClient(conf)) {
      path = normalizeFileSystemPath(path);
      MountTableManager mountTable = client.getMountTableManager();
      GetDestinationRequest request =
          GetDestinationRequest.newInstance(path);
      GetDestinationResponse response = mountTable.getDestination(request);
      System.out.println("Destination: " +
          StringUtils.join(",", response.getDestinations()));
    }
    return 0;
  }

  /**
   * Set quota for a mount table entry.
   *
   * @param conf The configuration to use
   * @param args Parameters of the quota.
   */
  private int setQuota(Configuration conf, List<String> args) throws IOException {
    final String DONT_SET = Long.toString(HdfsConstants.QUOTA_DONT_SET);
    long nsQuota = Long.parseLong(StringUtils.popOptionWithArgument(
        "-nsQuota", args, DONT_SET));
    long ssQuota = StringUtils.TraditionalBinaryPrefix.string2long(
        StringUtils.popOptionWithArgument("-ssQuota", args, DONT_SET));
    String mount = StringUtils.popFirstNonOption(args);
    StringUtils.ensureAllUsed(args);

    if (nsQuota <= 0 || ssQuota <= 0) {
      throw new IllegalArgumentException(
          "Input quota value should be a positive number.");
    }

    if (nsQuota == HdfsConstants.QUOTA_DONT_SET &&
        ssQuota == HdfsConstants.QUOTA_DONT_SET) {
      throw new IllegalArgumentException(
          "Must specify at least one of -nsQuota and -ssQuota.");
    }

    return updateQuota(conf, mount, nsQuota, ssQuota) ? 0 : -1;
  }

  /**
   * Set storage type quota for a mount table entry.
   *
   * @param conf The configuration to use
   * @param args Parameters of the quota.
   */
  private int setStorageTypeQuota(Configuration conf,
                                  List<String> args) throws IOException {
    long[] typeQuota = new long[StorageType.values().length];
    eachByStorageType(
        t -> typeQuota[t.ordinal()] = HdfsConstants.QUOTA_DONT_SET);

    String storageStr = StringUtils.popOptionWithArgument("-storageType", args);
    if (storageStr == null) {
      throw new IllegalArgumentException("Missing -storageType parameter");
    }
    StorageType type = StorageType.parseStorageType(storageStr);
    String mount = StringUtils.popFirstNonOption(args);
    long size = StringUtils.TraditionalBinaryPrefix.string2long(
        StringUtils.popFirstNonOption(args));
    typeQuota[type.ordinal()] = size;

    if (orByStorageType(t -> typeQuota[t.ordinal()] <= 0)) {
      throw new IllegalArgumentException(
          "Input quota value should be a positive number.");
    }

    if (andByStorageType(
        t -> typeQuota[t.ordinal()] == HdfsConstants.QUOTA_DONT_SET)) {
      throw new IllegalArgumentException(
          "Must specify at least one of -nsQuota and -ssQuota.");
    }

    return updateStorageTypeQuota(conf, mount, typeQuota) ? 0 : 1;
  }

  /**
   * Clear quota of the mount point.
   *
   * @param conf The configuration
   * @param args The list of arguments
   * @return 0 if the quota was cleared.
   * @throws IOException Error clearing the mount point.
   */
  private int clrQuota(Configuration conf, List<String> args) throws IOException {
    String mount = StringUtils.popFirstNonOption(args);
    if (mount == null) {
      throw new IllegalArgumentException("Missing required parameter.");
    }
    while (mount != null) {
      if (!updateQuota(conf, mount, HdfsConstants.QUOTA_RESET,
          HdfsConstants.QUOTA_RESET)) {
        System.err.println("Failed to clear quota for mount point " + mount);
        return -1;
      }
      System.out.println("Successfully clear quota for mount point " + mount);
      mount = StringUtils.popFirstNonOption(args);
    }
    return 0;
  }

  /**
   * Clear storage type quota of the mount point.
   *
   * @param conf The configuration
   * @param args The list of arguments
   * @return 0 if the quota was cleared.
   * @throws IOException Error clearing the mount point.
   */
  private int clrStorageTypeQuota(Configuration conf,
                                  List<String> args) throws IOException {
    String mount = StringUtils.popFirstNonOption(args);
    if (mount == null) {
      throw new IllegalArgumentException("Missing required parameter.");
    }
    while (mount != null) {
      long[] typeQuota = new long[StorageType.values().length];
      eachByStorageType(t -> typeQuota[t.ordinal()] = HdfsConstants.QUOTA_RESET);
      if (!updateStorageTypeQuota(conf, mount, typeQuota)) {
        System.err.println("Failed to clear storage type quota for mount point "
            + mount);
        return -1;
      }
      System.out.println("Successfully clear storage type quota for mount point "
          + mount);
      mount = StringUtils.popFirstNonOption(args);
    }
    return 0;
  }

  /**
   * Update quota of specified mount table.
   * @param conf The configuration
   * @param mount Specified mount table to update.
   * @param nsQuota Namespace quota.
   * @param ssQuota Storage space quota.
   * @return If the quota was updated.
   * @throws IOException Error updating quota.
   */
  private boolean updateQuota(Configuration conf, String mount, long nsQuota,
                              long ssQuota) throws IOException {
    // Get existing entry
    try (RouterClient client = createClient(conf)) {
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
        // If nsQuota and ssQuota were reset, clear nsQuota and ssQuota.
        if (nsQuota == HdfsConstants.QUOTA_RESET &&
            ssQuota == HdfsConstants.QUOTA_RESET) {
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
  }

  /**
   * Update storage type quota of specified mount table.
   * @param conf The configuration
   * @param mount Specified mount table to update.
   * @param typeQuota Storage type quota.
   * @return If the quota was updated.
   * @throws IOException Error updating quota.
   */
  private boolean updateStorageTypeQuota(Configuration conf,
                                         String mount,
                                         long[] typeQuota) throws IOException {
    // Get existing entry
    try (RouterClient client = createClient(conf)) {
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
        final RouterQuotaUsage quotaUsage = existingEntry.getQuota();
        long[] typeCount = new long[StorageType.values().length];
        eachByStorageType(
            t -> typeCount[t.ordinal()] = quotaUsage.getTypeQuota(t));
        // If all storage type quota were reset, clear the storage type quota.
        if (andByStorageType(
            t -> typeQuota[t.ordinal()] == HdfsConstants.QUOTA_RESET)) {
          eachByStorageType(t -> typeCount[t.ordinal()] =
              RouterQuotaUsage.QUOTA_USAGE_COUNT_DEFAULT);
        } else {
          // If nsQuota or ssQuota was unset, use the value in mount table.
          eachByStorageType(t -> {
            if (typeQuota[t.ordinal()] == HdfsConstants.QUOTA_DONT_SET) {
              typeQuota[t.ordinal()] = quotaUsage.getTypeQuota(t);
            }
          });
        }

        RouterQuotaUsage updatedQuota = new RouterQuotaUsage.Builder()
            .typeQuota(typeQuota).typeConsumed(typeCount).build();
        existingEntry.setQuota(updatedQuota);
      }

      UpdateMountTableEntryRequest updateRequest =
          UpdateMountTableEntryRequest.newInstance(existingEntry);
      UpdateMountTableEntryResponse updateResponse = mountTable
          .updateMountTableEntry(updateRequest);
      return updateResponse.getStatus();
    }
  }

  /**
   * Manager the safe mode state.
   * @param args Input command, enter or leave safe mode.
   */
  private int manageSafeMode(Configuration conf, List<String> args) throws IOException {
    String cmd = StringUtils.popFirstNonOption(args);
    StringUtils.ensureAllUsed(args);
    if (cmd == null) {
      throw new IllegalArgumentException("Please specify command: enter, leave, or get");
    }
    switch (cmd) {
      case "enter":
        if (enterSafeMode(conf)) {
          System.out.println("Successfully enter safe mode.");
          return 0;
        }
        break;
      case "leave":
        if (leaveSafeMode(conf)) {
          System.out.println("Successfully leave safe mode.");
          return 0;
        }
        break;
      case "get":
        boolean result = getSafeMode(conf);
        System.out.println("Safe Mode: " + result);
        return 0;
      default:
        throw new IllegalArgumentException("Invalid argument: " + cmd);
    }
    return 1;
  }

  /**
   * Request the Router enter safemode state.
   * @return Was operation successful?
   */
  private boolean enterSafeMode(Configuration conf) throws IOException {
    try (RouterClient client = createClient(conf)) {
      RouterStateManager stateManager = client.getRouterStateManager();
      return stateManager.enterSafeMode(EnterSafeModeRequest.newInstance())
          .getStatus();
    }
  }

  /**
   * Request the Router leave safemode state.
   * @return Was operation successful?
   */
  private boolean leaveSafeMode(Configuration conf) throws IOException {
    try (RouterClient client = createClient(conf)) {
      RouterStateManager stateManager = client.getRouterStateManager();
      return stateManager.leaveSafeMode(LeaveSafeModeRequest.newInstance())
          .getStatus();
    }
  }

  /**
   * Verify if current Router state is safe mode state.
   * @return True if the Router is in safe mode.
   */
  private boolean getSafeMode(Configuration conf) throws IOException {
    try (RouterClient client = createClient(conf)) {
      RouterStateManager stateManager = client.getRouterStateManager();
      return stateManager.getSafeMode(GetSafeModeRequest.newInstance())
          .isInSafeMode();
    }
  }

  /**
   * Manage the name service: enabling/disabling.
   * @param args Input command, disable or enable.
   */
  private int manageNameservice(Configuration conf, List<String> args) throws IOException {
    String cmd = StringUtils.popFirstNonOption(args);
    String nsId = StringUtils.popFirstNonOption(args);
    StringUtils.ensureAllUsed(args);
    if (cmd == null || nsId == null) {
      throw new IllegalArgumentException("Too few parameters for command");
    }
    switch (cmd) {
      case "enable":
        if (enableNameservice(conf, nsId)) {
          System.out.println("Successfully enabled nameservice " + nsId);
          return 0;
        } else {
          System.err.println("Cannot enable " + nsId);
          return 1;
        }
      case "disable":
        if (disableNameservice(conf, nsId)) {
          System.out.println("Successfully disabled nameservice " + nsId);
          return 0;
        } else {
          System.err.println("Cannot disable " + nsId);
          return 1;
        }
      default:
        throw new IllegalArgumentException("Unknown command: " + cmd);
    }
  }

  private boolean disableNameservice(Configuration conf,
                                     String nsId) throws IOException {
    try (RouterClient client = createClient(conf)) {
      NameserviceManager nameserviceManager = client.getNameserviceManager();
      DisableNameserviceResponse response =
          nameserviceManager.disableNameservice(
              DisableNameserviceRequest.newInstance(nsId));
      return response.getStatus();
    }
  }

  private boolean enableNameservice(Configuration conf,
                                    String nsId) throws IOException {
    try (RouterClient client = createClient(conf)) {
      NameserviceManager nameserviceManager = client.getNameserviceManager();
      EnableNameserviceResponse response =
          nameserviceManager.enableNameservice(
              EnableNameserviceRequest.newInstance(nsId));
      return response.getStatus();
    }
  }

  private int getDisabledNameservices(Configuration conf,
                                      List<String> args) throws IOException {
    StringUtils.ensureAllUsed(args);
    try (RouterClient client = createClient(conf)) {
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
    return 0;
  }

  public int genericRefresh(Configuration conf, List<String> args) throws IOException {
    String hostport = StringUtils.popFirstNonOption(args);
    String identifier = StringUtils.popFirstNonOption(args);
    String[] remaining = args.toArray(new String[]{});

    // for security authorization
    // server principal for this call
    // should be NN's one.
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        conf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, ""));

    // Create the client
    Class<?> xface = GenericRefreshProtocolPB.class;
    InetSocketAddress address = NetUtils.createSocketAddr(hostport);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    RPC.setProtocolEngine(conf, xface, ProtobufRpcEngine2.class);
    GenericRefreshProtocolPB proxy = (GenericRefreshProtocolPB)RPC.getProxy(
        xface, RPC.getProtocolVersion(xface), address, ugi, conf,
        NetUtils.getDefaultSocketFactory(conf), 0);

    Collection<RefreshResponse> responses;
    try (GenericRefreshProtocolClientSideTranslatorPB xlator =
        new GenericRefreshProtocolClientSideTranslatorPB(proxy)) {
      // Refresh
      responses = xlator.refresh(identifier, remaining);

      int returnCode = 0;

      // Print refresh responses
      System.out.println("Refresh Responses:\n");
      for (RefreshResponse response : responses) {
        System.out.println(response.toString());

        if (returnCode == 0 && response.getReturnCode() != 0) {
          // This is the first non-zero return code, so we should return this
          returnCode = response.getReturnCode();
        } else if (returnCode != 0 && response.getReturnCode() != 0) {
          // Then now we have multiple non-zero return codes,
          // so we merge them into -1
          returnCode = -1;
        }
      }
      return returnCode;
    }
  }

  /**
   * Refresh Router's call Queue.
   *
   * @throws IOException if the operation was not successful.
   */
  private int refreshCallQueue(Configuration conf,
                               List<String> args) throws IOException {
    StringUtils.ensureAllUsed(args);
    String hostport =  getAddress(conf);

    // Create the client
    Class<?> xface = RefreshCallQueueProtocolPB.class;
    InetSocketAddress address = NetUtils.createSocketAddr(hostport);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    RPC.setProtocolEngine(conf, xface, ProtobufRpcEngine2.class);
    RefreshCallQueueProtocolPB proxy = (RefreshCallQueueProtocolPB)RPC.getProxy(
        xface, RPC.getProtocolVersion(xface), address, ugi, conf,
        NetUtils.getDefaultSocketFactory(conf), 0);

    int returnCode = -1;
    try (RefreshCallQueueProtocolClientSideTranslatorPB xlator =
        new RefreshCallQueueProtocolClientSideTranslatorPB(proxy)) {
      xlator.refreshCallQueue();
      System.out.println("Refresh call queue successfully for " + hostport);
      returnCode = 0;
    } catch (IOException ioe){
      System.out.println("Refresh call queue unsuccessfully for " + hostport);
    }
    return returnCode;
  }

  /**
   * Dumps the contents of the StateStore to stdout.
   * @param conf The configuration
   * @param args The arguments to the command
   * @return 0 if it was successful
   */
  public static int dumpStateStore(Configuration conf,
                                   List<String> args) throws IOException {
    StringUtils.ensureAllUsed(args);
    StateStoreService service = new StateStoreService();
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_METRICS_ENABLE, false);
    service.init(conf);
    service.loadDriver();
    if (!service.isDriverReady()) {
      System.err.println("Can't initialize driver");
      return -1;
    }
    // Get the stores sorted by name
    Map<String, RecordStore<? extends BaseRecord>> stores = new TreeMap<>();
    for(RecordStore<? extends BaseRecord> store: service.getRecordStores()) {
      String recordName = StateStoreUtils.getRecordName(store.getRecordClass());
      stores.put(recordName, store);
    }
    for (Entry<String, RecordStore<? extends BaseRecord>> pair: stores.entrySet()) {
      String recordName = pair.getKey();
      RecordStore<? extends BaseRecord> store = pair.getValue();
      System.out.println("---- " + recordName + " ----");
      if (store instanceof CachedRecordStore) {
        for (Object record: ((CachedRecordStore) store).getCachedRecords()) {
          if (record instanceof BaseRecord && record instanceof PBRecord) {
            BaseRecord baseRecord = (BaseRecord) record;
            // Generate the pseudo-json format of the protobuf record
            String recordString = ((PBRecord) record).getProto().toString();
            // Indent each line
            recordString = "    " + recordString.replaceAll("\n", "\n    ");
            System.out.printf("  %s:%n", baseRecord.getPrimaryKey());
            System.out.println(recordString);
          }
        }
        System.out.println();
      }
    }
    service.stop();
    return 0;
  }

  /**
   * Normalize a path for that filesystem.
   *
   * @param str Path to normalize. The path doesn't have scheme or authority.
   * @return Normalized path.
   */
  public static String normalizeFileSystemPath(final String str) {
    String path = SLASHES.matcher(str).replaceAll("/");
    if (path.length() > 1 && path.endsWith("/")) {
      path = path.substring(0, path.length()-1);
    }
    return path;
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
