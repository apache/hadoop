/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;

/**
 * Module that implements all the RPC calls related to snapshots in
 * {@link ClientProtocol} in the {@link RouterRpcServer}.
 */
public class RouterSnapshot {

  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Find generic locations. */
  private final ActiveNamenodeResolver namenodeResolver;

  public RouterSnapshot(RouterRpcServer server) {
    this.rpcServer = server;
    this.rpcClient = this.rpcServer.getRPCClient();
    this.namenodeResolver = rpcServer.getNamenodeResolver();
  }

  public void allowSnapshot(String snapshotRoot) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod method = new RemoteMethod("allowSnapshot",
        new Class<?>[] {String.class}, new RemoteParam());

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  public void disallowSnapshot(String snapshotRoot) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod method = new RemoteMethod("disallowSnapshot",
        new Class<?>[] {String.class}, new RemoteParam());
    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod method = new RemoteMethod("createSnapshot",
        new Class<?>[] {String.class, String.class}, new RemoteParam(),
        snapshotName);

    String result = null;
    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      Map<RemoteLocation, String> results = rpcClient.invokeConcurrent(
          locations, method, String.class);
      Entry<RemoteLocation, String> firstelement =
          results.entrySet().iterator().next();
      RemoteLocation loc = firstelement.getKey();
      result = firstelement.getValue();
      result = result.replaceFirst(loc.getDest(), loc.getSrc());
    } else {
      result = rpcClient.invokeSequential(
          locations, method, String.class, null);
      RemoteLocation loc = locations.get(0);
      result = result.replaceFirst(loc.getDest(), loc.getSrc());
    }
    return result;
  }

  public void deleteSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod method = new RemoteMethod("deleteSnapshot",
        new Class<?>[] {String.class, String.class},
        new RemoteParam(), snapshotName);

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  public void renameSnapshot(String snapshotRoot, String oldSnapshotName,
      String newSnapshot) throws IOException {
    rpcServer.checkOperation(OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod method = new RemoteMethod("renameSnapshot",
        new Class<?>[] {String.class, String.class, String.class},
        new RemoteParam(), oldSnapshotName, newSnapshot);

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(locations, method);
    } else {
      rpcClient.invokeSequential(locations, method);
    }
  }

  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getSnapshottableDirListing");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, SnapshottableDirectoryStatus[]> ret =
        rpcClient.invokeConcurrent(
            nss, method, true, false, SnapshottableDirectoryStatus[].class);

    return RouterRpcServer.merge(ret, SnapshottableDirectoryStatus.class);
  }

  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName)
          throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod remoteMethod = new RemoteMethod("getSnapshotDiffReport",
        new Class<?>[] {String.class, String.class, String.class},
        new RemoteParam(), earlierSnapshotName, laterSnapshotName);

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      Map<RemoteLocation, SnapshotDiffReport> ret = rpcClient.invokeConcurrent(
          locations, remoteMethod, true, false, SnapshotDiffReport.class);
      return ret.values().iterator().next();
    } else {
      return rpcClient.invokeSequential(
          locations, remoteMethod, SnapshotDiffReport.class, null);
    }
  }

  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String earlierSnapshotName, String laterSnapshotName,
      byte[] startPath, int index) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    Class<?>[] params = new Class<?>[] {
        String.class, String.class, String.class,
        byte[].class, int.class};
    RemoteMethod remoteMethod = new RemoteMethod(
        "getSnapshotDiffReportListing", params,
        new RemoteParam(), earlierSnapshotName, laterSnapshotName,
        startPath, index);

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      Map<RemoteLocation, SnapshotDiffReportListing> ret =
          rpcClient.invokeConcurrent(locations, remoteMethod, false, false,
              SnapshotDiffReportListing.class);
      Collection<SnapshotDiffReportListing> listings = ret.values();
      SnapshotDiffReportListing listing0 = listings.iterator().next();
      return listing0;
    } else {
      return rpcClient.invokeSequential(
          locations, remoteMethod, SnapshotDiffReportListing.class, null);
    }
  }
}