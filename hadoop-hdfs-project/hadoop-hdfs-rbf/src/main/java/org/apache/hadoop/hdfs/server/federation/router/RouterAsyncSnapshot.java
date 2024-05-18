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

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.asyncRequestThenApply;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.asyncReturn;

public class RouterAsyncSnapshot extends RouterSnapshot{
  public RouterAsyncSnapshot(RouterRpcServer server) {
    super(server);
  }

  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    RouterRpcClient rpcClient = getRpcClient();
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod method = new RemoteMethod("createSnapshot",
        new Class<?>[] {String.class, String.class}, new RemoteParam(),
        snapshotName);

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      return asyncRequestThenApply(
          () -> rpcClient.invokeConcurrent(locations, method, String.class),
          results -> {
            Map.Entry<RemoteLocation, String> firstelement =
                results.entrySet().iterator().next();
            RemoteLocation loc = firstelement.getKey();
            String result = firstelement.getValue();
            result = result.replaceFirst(loc.getDest(), loc.getSrc());
            return result;
          }, String.class);
    } else {
      return asyncRequestThenApply(
          () -> rpcClient.invokeSequential(method, locations,
              String.class, null),
          response -> {
            RemoteLocation loc = (RemoteLocation) response.getLocation();
            String invokedResult = (String) response.getResult();
            return invokedResult.replaceFirst(loc.getDest(), loc.getSrc());
          }, String.class);
    }
  }

  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    RouterRpcClient rpcClient = getRpcClient();
    RouterRpcServer rpcServer = getRpcServer();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getSnapshottableDirListing");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    return asyncRequestThenApply(
        () -> rpcClient.invokeConcurrent(nss, method, true,
            false, SnapshottableDirectoryStatus[].class),
        ret -> RouterRpcServer.merge(ret, SnapshottableDirectoryStatus.class),
        SnapshottableDirectoryStatus[].class);
  }

  public SnapshotStatus[] getSnapshotListing(String snapshotRoot)
      throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod remoteMethod = new RemoteMethod("getSnapshotListing",
        new Class<?>[]{String.class},
        new RemoteParam());

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      return asyncRequestThenApply(
          () -> rpcClient.invokeConcurrent(locations, remoteMethod, true,
              false, SnapshotStatus[].class),
          ret -> {
            SnapshotStatus[] response = ret.values().iterator().next();
            String src = ret.keySet().iterator().next().getSrc();
            String dst = ret.keySet().iterator().next().getDest();
            for (SnapshotStatus s : response) {
              String mountPath = DFSUtil.bytes2String(s.getParentFullPath()).
                  replaceFirst(src, dst);
              s.setParentFullPath(DFSUtil.string2Bytes(mountPath));
            }
            return response;
          }, SnapshotStatus[].class);
    } else {
      return asyncRequestThenApply(
          () -> rpcClient.invokeSequential(remoteMethod, locations,
              SnapshotStatus[].class, null),
          invokedResponse -> {
            RemoteLocation loc = (RemoteLocation) invokedResponse.getLocation();
            SnapshotStatus[] response = (SnapshotStatus[]) invokedResponse.getResult();
            for (SnapshotStatus s : response) {
              String mountPath = DFSUtil.bytes2String(s.getParentFullPath()).
                  replaceFirst(loc.getDest(), loc.getSrc());
              s.setParentFullPath(DFSUtil.string2Bytes(mountPath));
            }
            return response;
          }, SnapshotStatus[].class);
    }
  }

  public SnapshotDiffReport getSnapshotDiffReport(
      String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    RouterRpcClient rpcClient = getRpcClient();
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod remoteMethod = new RemoteMethod("getSnapshotDiffReport",
        new Class<?>[] {String.class, String.class, String.class},
        new RemoteParam(), earlierSnapshotName, laterSnapshotName);

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      return asyncRequestThenApply(
          () -> rpcClient.invokeConcurrent(locations, remoteMethod,
              true, false, SnapshotDiffReport.class),
          ret -> ret.values().iterator().next(), SnapshotDiffReport.class);
    } else {
      rpcClient.invokeSequential(
          locations, remoteMethod, SnapshotDiffReport.class, null);
      return asyncReturn(SnapshotDiffReport.class);
    }
  }

  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String earlierSnapshotName, String laterSnapshotName,
      byte[] startPath, int index) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
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
      return asyncRequestThenApply(() -> rpcClient.invokeConcurrent(locations, remoteMethod,
          false, false, SnapshotDiffReportListing.class),
          ret -> {
            Collection<SnapshotDiffReportListing> listings = ret.values();
            return listings.iterator().next();
          }, SnapshotDiffReportListing.class);
    } else {
      rpcClient.invokeSequential(
          locations, remoteMethod, SnapshotDiffReportListing.class, null);
      return asyncReturn(SnapshotDiffReportListing.class);
    }
  }
}
