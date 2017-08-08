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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
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
        + "[-readonly] [-order HASH|LOCAL|RANDOM|HASH_ALL]]\n"
        + "\t[-rm <source>]\n"
        + "\t[-ls <path>]\n";
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
    }

    // Initialize RouterClient
    try {
      String address = getConf().getTrimmed(
          DFSConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
          DFSConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT);
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
          System.err.println("Successfuly added mount point " + argv[i]);
        }
      } else if ("-rm".equals(cmd)) {
        if (removeMount(argv[i])) {
          System.err.println("Successfully removed mount point " + argv[i]);
        }
      } else if ("-ls".equals(cmd)) {
        if (argv.length > 1) {
          listMounts(argv[i]);
        } else {
          listMounts("/");
        }
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
      }
      i++;
    }

    return addMount(mount, nss, dest, readOnly, order);
  }

  /**
   * Add a mount table entry or update if it exists.
   *
   * @param mount Mount point.
   * @param nss Namespaces where this is mounted to.
   * @param dest Destination path.
   * @param readonly If the mount point is read only.
   * @param order Order of the destination locations.
   * @return If the mount point was added.
   * @throws IOException Error adding the mount point.
   */
  public boolean addMount(String mount, String[] nss, String dest,
      boolean readonly, DestinationOrder order) throws IOException {
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
        "%-25s %-25s",
        "Source", "Destinations"));
    for (MountTable entry : entries) {
      StringBuilder destBuilder = new StringBuilder();
      for (RemoteLocation location : entry.getDestinations()) {
        if (destBuilder.length() > 0) {
          destBuilder.append(",");
        }
        destBuilder.append(String.format("%s->%s", location.getNameserviceId(),
            location.getDest()));
      }
      System.out.println(String.format("%-25s %-25s", entry.getSourcePath(),
          destBuilder.toString()));
    }
  }
}