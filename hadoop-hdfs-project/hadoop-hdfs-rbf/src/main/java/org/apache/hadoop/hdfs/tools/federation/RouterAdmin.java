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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolClientSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB;
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

/**
 * This class provides some Federation administrative access shell commands.
 */
@Private
public class RouterAdmin extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(RouterAdmin.class);

  private RouterClient client;

  /** Pre-compiled regular expressions to detect duplicated slashes. */
  private static final Pattern SLASHES = Pattern.compile("/+");

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
    String usage = getUsage(null);
    System.out.println(usage);
  }

  private void printUsage(String cmd) {
    String usage = getUsage(cmd);
    System.out.println(usage);
  }

  private String getUsage(String cmd) {
    if (cmd == null) {
      String[] commands =
          {"-add", "-update", "-rm", "-ls", "-getDestination", "-setQuota",
              "-setStorageTypeQuota", "-clrQuota", "-clrStorageTypeQuota",
              "-safemode", "-nameservice", "-getDisabledNameservices",
              "-refresh", "-refreshRouterArgs",
              "-refreshSuperUserGroupsConfiguration"};
      StringBuilder usage = new StringBuilder();
      usage.append("Usage: hdfs dfsrouteradmin :\n");
      for (int i = 0; i < commands.length; i++) {
        usage.append(getUsage(commands[i]));
        if (i + 1 < commands.length) {
          usage.append("\n");
        }
      }
      return usage.toString();
    }
    if (cmd.equals("-add")) {
      return "\t[-add <source> <nameservice1, nameservice2, ...> <destination> "
          + "[-readonly] [-faulttolerant] "
          + "[-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] "
          + "-owner <owner> -group <group> -mode <mode>]";
    } else if (cmd.equals("-update")) {
      return "\t[-update <source>"
          + " [<nameservice1, nameservice2, ...> <destination>] "
          + "[-readonly true|false] [-faulttolerant true|false] "
          + "[-order HASH|LOCAL|RANDOM|HASH_ALL|SPACE] "
          + "-owner <owner> -group <group> -mode <mode>]";
    } else if (cmd.equals("-rm")) {
      return "\t[-rm <source>]";
    } else if (cmd.equals("-ls")) {
      return "\t[-ls [-d] <path>]";
    } else if (cmd.equals("-getDestination")) {
      return "\t[-getDestination <path>]";
    } else if (cmd.equals("-setQuota")) {
      return "\t[-setQuota <path> -nsQuota <nsQuota> -ssQuota "
          + "<quota in bytes or quota size string>]";
    } else if (cmd.equals("-setStorageTypeQuota")) {
      return "\t[-setStorageTypeQuota <path> -storageType <storage type> "
          + "<quota in bytes or quota size string>]";
    } else if (cmd.equals("-clrQuota")) {
      return "\t[-clrQuota <path>]";
    } else if (cmd.equals("-clrStorageTypeQuota")) {
      return "\t[-clrStorageTypeQuota <path>]";
    } else if (cmd.equals("-safemode")) {
      return "\t[-safemode enter | leave | get]";
    } else if (cmd.equals("-nameservice")) {
      return "\t[-nameservice enable | disable <nameservice>]";
    } else if (cmd.equals("-getDisabledNameservices")) {
      return "\t[-getDisabledNameservices]";
    } else if (cmd.equals("-refresh")) {
      return "\t[-refresh]";
    } else if (cmd.equals("-refreshRouterArgs")) {
      return "\t[-refreshRouterArgs <host:ipc_port> <key> [arg1..argn]]";
    } else if (cmd.equals("-refreshSuperUserGroupsConfiguration")) {
      return "\t[-refreshSuperUserGroupsConfiguration]";
    }
    return getUsage(null);
  }

  /**
   * Usage: validates the maximum number of arguments for a command.
   * @param arg List of of command line parameters.
   */
  private void validateMax(String[] arg) {
    if (arg[0].equals("-ls")) {
      if (arg.length > 3) {
        throw new IllegalArgumentException(
            "Too many arguments, Max=2 argument allowed");
      }
    } else if (arg[0].equals("-getDestination")) {
      if (arg.length > 2) {
        throw new IllegalArgumentException(
            "Too many arguments, Max=1 argument allowed only");
      }
    } else if (arg[0].equals("-safemode")) {
      if (arg.length > 2) {
        throw new IllegalArgumentException(
            "Too many arguments, Max=1 argument allowed only");
      }
    } else if (arg[0].equals("-nameservice")) {
      if (arg.length > 3) {
        throw new IllegalArgumentException(
            "Too many arguments, Max=2 arguments allowed");
      }
    } else if (arg[0].equals("-getDisabledNameservices")) {
      if (arg.length > 1) {
        throw new IllegalArgumentException("No arguments allowed");
      }
    } else if (arg[0].equals("-refreshSuperUserGroupsConfiguration")) {
      if (arg.length > 1) {
        throw new IllegalArgumentException("No arguments allowed");
      }
    }
  }

  /**
   * Usage: validates the minimum number of arguments for a command.
   * @param argv List of of command line parameters.
   * @return true if number of arguments are valid for the command else false.
   */
  private boolean validateMin(String[] argv) {
    String cmd = argv[0];
    if ("-add".equals(cmd)) {
      if (argv.length < 4) {
        return false;
      }
    } else if ("-update".equals(cmd)) {
      if (argv.length < 4) {
        return false;
      }
    } else if ("-rm".equals(cmd)) {
      if (argv.length < 2) {
        return false;
      }
    } else if ("-getDestination".equals(cmd)) {
      if (argv.length < 2) {
        return false;
      }
    } else if ("-setQuota".equals(cmd)) {
      if (argv.length < 4) {
        return false;
      }
    } else if ("-setStorageTypeQuota".equals(cmd)) {
      if (argv.length < 5) {
        return false;
      }
    } else if ("-clrQuota".equals(cmd)) {
      if (argv.length < 2) {
        return false;
      }
    } else if ("-clrStorageTypeQuota".equals(cmd)) {
      if (argv.length < 2) {
        return false;
      }
    } else if ("-safemode".equals(cmd)) {
      if (argv.length < 2) {
        return false;
      }
    } else if ("-nameservice".equals(cmd)) {
      if (argv.length < 3) {
        return false;
      }
    } else if ("-refreshRouterArgs".equals(cmd)) {
      if (argv.length < 2) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int run(String[] argv) throws Exception {
    if (argv.length < 1) {
      System.err.println("Not enough parameters specified");
      printUsage();
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    // Verify that we have enough command line parameters
    if (!validateMin(argv)) {
      System.err.println("Not enough parameters specificed for cmd " + cmd);
      printUsage(cmd);
      return exitCode;
    }
    String address = null;
    // Initialize RouterClient
    try {
      address = getConf().getTrimmed(
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
      validateMax(argv);
      if ("-add".equals(cmd)) {
        if (addMount(argv, i)) {
          System.out.println("Successfully added mount point " + argv[i]);
        } else {
          exitCode = -1;
        }
      } else if ("-update".equals(cmd)) {
        if (updateMount(argv, i)) {
          System.out.println("Successfully updated mount point " + argv[i]);
          System.out.println(
              "WARN: Changing order/destinations may lead to inconsistencies");
        } else {
          exitCode = -1;
        }
      } else if ("-rm".equals(cmd)) {
        while (i < argv.length) {
          try {
            if (removeMount(argv[i])) {
              System.out.println("Successfully removed mount point " + argv[i]);
            }
          } catch (IOException e) {
            exitCode = -1;
            System.err
                .println(cmd.substring(1) + ": " + e.getLocalizedMessage());
          }
          i++;
        }
      } else if ("-ls".equals(cmd)) {
        listMounts(argv, i);
      } else if ("-getDestination".equals(cmd)) {
        getDestination(argv[i]);
      } else if ("-setQuota".equals(cmd)) {
        if (setQuota(argv, i)) {
          System.out.println(
              "Successfully set quota for mount point " + argv[i]);
        }
      } else if ("-setStorageTypeQuota".equals(cmd)) {
        if (setStorageTypeQuota(argv, i)) {
          System.out.println(
              "Successfully set storage type quota for mount point " + argv[i]);
        }
      } else if ("-clrQuota".equals(cmd)) {
        while (i < argv.length) {
          if (clrQuota(argv[i])) {
            System.out
                .println("Successfully clear quota for mount point " + argv[i]);
            i++;
          }
        }
      } else if ("-clrStorageTypeQuota".equals(cmd)) {
        while (i < argv.length) {
          if (clrStorageTypeQuota(argv[i])) {
            System.out.println("Successfully clear storage type quota for mount"
                + " point " + argv[i]);
            i++;
          }
        }
      } else if ("-safemode".equals(cmd)) {
        manageSafeMode(argv[i]);
      } else if ("-nameservice".equals(cmd)) {
        String subcmd = argv[i];
        String nsId = argv[i + 1];
        manageNameservice(subcmd, nsId);
      } else if ("-getDisabledNameservices".equals(cmd)) {
        getDisabledNameservices();
      } else if ("-refresh".equals(cmd)) {
        refresh(address);
      } else if ("-refreshRouterArgs".equals(cmd)) {
        exitCode = genericRefresh(argv, i);
      } else if ("-refreshSuperUserGroupsConfiguration".equals(cmd)) {
        exitCode = refreshSuperUserGroupsConfiguration();
      } else {
        throw new IllegalArgumentException("Unknown Command: " + cmd);
      }
    } catch (IllegalArgumentException arge) {
      debugException = arge;
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
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
    } catch (IOException ioe) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + ioe.getLocalizedMessage());
      printUsage(cmd);
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
   * Refresh superuser proxy groups mappings on Router.
   *
   * @throws IOException if the operation was not successful.
   */
  private int refreshSuperUserGroupsConfiguration()
      throws IOException{
    RouterGenericManager proxy = client.getRouterGenericManager();
    String address =  getConf().getTrimmed(
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
    if(proxy.refreshSuperUserGroupsConfiguration()){
      System.out.println(
          "Successfully updated superuser proxy groups on router " + address);
      return 0;
    }
    return -1;
  }

  private void refresh(String address) throws IOException {
    if (refreshRouterCache()) {
      System.out.println(
          "Successfully updated mount table cache on router " + address);
    }
  }

  /**
   * Refresh mount table cache on connected router.
   *
   * @return true if cache refreshed successfully
   * @throws IOException
   */
  private boolean refreshRouterCache() throws IOException {
    RefreshMountTableEntriesResponse response =
        client.getMountTableManager().refreshMountTableEntries(
            RefreshMountTableEntriesRequest.newInstance());
    return response.getResult();
  }


  /**
   * Add a mount table entry or update if it exists.
   *
   * @param parameters Parameters for the mount point.
   * @param i Index in the parameters.
   * @return If it was successful.
   * @throws IOException If it cannot add the mount point.
   */
  public boolean addMount(String[] parameters, int i) throws IOException {
    // Mandatory parameters
    String mount = parameters[i++];
    String[] nss = parameters[i++].split(",");
    String dest = parameters[i++];

    // Optional parameters
    boolean readOnly = false;
    boolean faultTolerant = false;
    String owner = null;
    String group = null;
    FsPermission mode = null;
    DestinationOrder order = DestinationOrder.HASH;
    while (i < parameters.length) {
      if (parameters[i].equals("-readonly")) {
        readOnly = true;
      } else if (parameters[i].equals("-faulttolerant")) {
        faultTolerant = true;
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
      } else {
        printUsage("-add");
        return false;
      }

      i++;
    }

    return addMount(mount, nss, dest, readOnly, faultTolerant, order,
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
      boolean readonly, boolean faultTolerant, DestinationOrder order,
      ACLEntity aclInfo)
      throws IOException {
    mount = normalizeFileSystemPath(mount);
    // Get the existing entry
    MountTableManager mountTable = client.getMountTableManager();
    MountTable existingEntry = getMountEntry(mount, mountTable);

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
      if (faultTolerant) {
        newEntry.setFaultTolerant(true);
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
      }
      return added;
    } else {
      // Update the existing entry if it exists
      for (String nsId : nss) {
        if (!existingEntry.addDestination(nsId, dest)) {
          System.err.println("Cannot add destination at " + nsId + " " + dest);
          return false;
        }
      }
      if (readonly) {
        existingEntry.setReadOnly(true);
      }
      if (faultTolerant) {
        existingEntry.setFaultTolerant(true);
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
      }
      return updated;
    }
  }

  /**
   * Update a mount table entry.
   *
   * @param parameters Parameters for the mount point.
   * @param i Index in the parameters.
   * @return If it updated the mount point successfully.
   * @throws IOException If there is an error.
   */
  public boolean updateMount(String[] parameters, int i) throws IOException {
    String mount = parameters[i++];
    mount = normalizeFileSystemPath(mount);
    MountTableManager mountTable = client.getMountTableManager();
    MountTable existingEntry = getMountEntry(mount, mountTable);
    if (existingEntry == null) {
      throw new IOException(mount + " doesn't exist.");
    }
    // Check if the destination needs to be updated.

    if (!parameters[i].startsWith("-")) {
      String[] nss = parameters[i++].split(",");
      String dest = parameters[i++];
      Map<String, String> destMap = new LinkedHashMap<>();
      for (String ns : nss) {
        destMap.put(ns, dest);
      }
      final List<RemoteLocation> locations = new LinkedList<>();
      for (Entry<String, String> entry : destMap.entrySet()) {
        String nsId = entry.getKey();
        String path = normalizeFileSystemPath(entry.getValue());
        RemoteLocation location = new RemoteLocation(nsId, path, mount);
        locations.add(location);
      }
      existingEntry.setDestinations(locations);
    }
    try {
      while (i < parameters.length) {
        switch (parameters[i]) {
        case "-readonly":
          i++;
          existingEntry.setReadOnly(getBooleanValue(parameters[i]));
          break;
        case "-faulttolerant":
          i++;
          existingEntry.setFaultTolerant(getBooleanValue(parameters[i]));
          break;
        case "-order":
          i++;
          try {
            existingEntry.setDestOrder(DestinationOrder.valueOf(parameters[i]));
            break;
          } catch (Exception e) {
            throw new Exception("Cannot parse order: " + parameters[i]);
          }
        case "-owner":
          i++;
          existingEntry.setOwnerName(parameters[i]);
          break;
        case "-group":
          i++;
          existingEntry.setGroupName(parameters[i]);
          break;
        case "-mode":
          i++;
          short modeValue = Short.parseShort(parameters[i], 8);
          existingEntry.setMode(new FsPermission(modeValue));
          break;
        default:
          printUsage("-update");
          return false;
        }
        i++;
      }
    } catch (IllegalArgumentException iae) {
      throw iae;
    } catch (Exception e) {
      String msg = "Unable to parse arguments: " + e.getMessage();
      if (e instanceof ArrayIndexOutOfBoundsException) {
        msg = "Unable to parse arguments: no value provided for "
            + parameters[i - 1];
      }
      throw new IOException(msg);
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

  /**
   * Parse string to boolean.
   * @param value the string to be parsed.
   * @return parsed boolean value.
   * @throws Exception if other than true|false is provided.
   */
  private boolean getBooleanValue(String value) throws Exception {
    if (value.equalsIgnoreCase("true")) {
      return true;
    } else if (value.equalsIgnoreCase("false")) {
      return false;
    }
    throw new IllegalArgumentException("Invalid argument: " + value
        + ". Please specify either true or false.");
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
   *
   * @param path Path to remove.
   * @return If the mount point was removed successfully.
   * @throws IOException If it cannot be removed.
   */
  public boolean removeMount(String path) throws IOException {
    path = normalizeFileSystemPath(path);
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
   * @param argv Parameters of the path.
   * @param i Index in the parameters.
   * @throws IOException If it cannot be listed.
   */
  public void listMounts(String[] argv, int i) throws IOException {
    String path;
    boolean detail = false;
    if (argv.length == 1) {
      path = "/";
    } else if (argv[i].equals("-d")) { // Check if -d parameter is specified.
      detail = true;
      if (argv.length == 2) {
        path = "/"; // If no path is provide with -ls -d.
      } else {
        path = argv[++i];
      }
    } else {
      path = argv[i];
    }

    path = normalizeFileSystemPath(path);
    MountTableManager mountTable = client.getMountTableManager();
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance(path);
    GetMountTableEntriesResponse response =
        mountTable.getMountTableEntries(request);
    List<MountTable> entries = response.getEntries();
    printMounts(entries, detail);
  }

  private static void printMounts(List<MountTable> entries, boolean detail) {
    System.out.println("Mount Table Entries:");
    if (detail) {
      System.out.println(
          String.format("%-25s %-25s %-25s %-25s %-10s %-30s %-10s %-10s %-15s",
              "Source", "Destinations", "Owner", "Group", "Mode", "Quota/Usage",
              "Order", "ReadOnly", "FaultTolerant"));
    } else {
      System.out.println(String.format("%-25s %-25s %-25s %-25s %-10s %-30s",
          "Source", "Destinations", "Owner", "Group", "Mode", "Quota/Usage"));
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
      System.out.print(String.format("%-25s %-25s", entry.getSourcePath(),
          destBuilder.toString()));

      System.out.print(String.format(" %-25s %-25s %-10s",
          entry.getOwnerName(), entry.getGroupName(), entry.getMode()));

      System.out.print(String.format(" %-30s", entry.getQuota()));

      if (detail) {
        System.out.print(String.format(" %-10s", entry.getDestOrder()));

        System.out.print(
            String.format(" %-10s", entry.isReadOnly() ? "Read-Only" : ""));

        System.out.print(String.format(" %-15s",
            entry.isFaultTolerant() ? "Fault-Tolerant" : ""));
      }
      System.out.println();
    }
  }

  private void getDestination(String path) throws IOException {
    path = normalizeFileSystemPath(path);
    MountTableManager mountTable = client.getMountTableManager();
    GetDestinationRequest request =
        GetDestinationRequest.newInstance(path);
    GetDestinationResponse response = mountTable.getDestination(request);
    System.out.println("Destination: " +
        StringUtils.join(",", response.getDestinations()));
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
      } else {
        throw new IllegalArgumentException(
            "Invalid argument : " + parameters[i]);
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
   * Set storage type quota for a mount table entry.
   *
   * @param parameters Parameters of the quota.
   * @param i Index in the parameters.
   */
  private boolean setStorageTypeQuota(String[] parameters, int i)
      throws IOException {
    long[] typeQuota = new long[StorageType.values().length];
    eachByStorageType(
        t -> typeQuota[t.ordinal()] = HdfsConstants.QUOTA_DONT_SET);

    String mount = parameters[i++];
    if (parameters[i].equals("-storageType")) {
      i++;
      StorageType type = StorageType.parseStorageType(parameters[i++]);
      typeQuota[type.ordinal()] = Long.parseLong(parameters[i]);
    } else {
      throw new IllegalArgumentException("Invalid argument : " + parameters[i]);
    }

    if (orByStorageType(t -> typeQuota[t.ordinal()] <= 0)) {
      throw new IllegalArgumentException(
          "Input quota value should be a positive number.");
    }

    if (andByStorageType(
        t -> typeQuota[t.ordinal()] == HdfsConstants.QUOTA_DONT_SET)) {
      throw new IllegalArgumentException(
          "Must specify at least one of -nsQuota and -ssQuota.");
    }

    return updateStorageTypeQuota(mount, typeQuota);
  }

  /**
   * Clear quota of the mount point.
   *
   * @param mount Mount table to clear
   * @return If the quota was cleared.
   * @throws IOException Error clearing the mount point.
   */
  private boolean clrQuota(String mount) throws IOException {
    return updateQuota(mount, HdfsConstants.QUOTA_RESET,
        HdfsConstants.QUOTA_RESET);
  }

  /**
   * Clear storage type quota of the mount point.
   *
   * @param mount Mount table to clear
   * @return If the quota was cleared.
   * @throws IOException Error clearing the mount point.
   */
  private boolean clrStorageTypeQuota(String mount) throws IOException {
    long[] typeQuota = new long[StorageType.values().length];
    eachByStorageType(t -> typeQuota[t.ordinal()] = HdfsConstants.QUOTA_RESET);
    return updateStorageTypeQuota(mount, typeQuota);
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

  /**
   * Update storage type quota of specified mount table.
   *
   * @param mount Specified mount table to update.
   * @param typeQuota Storage type quota.
   * @return If the quota was updated.
   * @throws IOException Error updating quota.
   */
  private boolean updateStorageTypeQuota(String mount, long[] typeQuota)
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
    } else {
      throw new IllegalArgumentException("Invalid argument: " + cmd);
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

  public int genericRefresh(String[] argv, int i) throws IOException {
    String hostport = argv[i++];
    String identifier = argv[i++];
    String[] args = Arrays.copyOfRange(argv, i, argv.length);

    // Get the current configuration
    Configuration conf = getConf();

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

    Collection<RefreshResponse> responses = null;
    try (GenericRefreshProtocolClientSideTranslatorPB xlator =
        new GenericRefreshProtocolClientSideTranslatorPB(proxy)) {
      // Refresh
      responses = xlator.refresh(identifier, args);

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
    } finally {
      if (responses == null) {
        System.out.println("Failed to get response.\n");
        return -1;
      }
    }
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