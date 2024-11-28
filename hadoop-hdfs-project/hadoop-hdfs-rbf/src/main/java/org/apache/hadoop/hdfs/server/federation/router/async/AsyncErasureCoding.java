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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.ErasureCoding;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RemoteParam;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.ApplyFunction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer.merge;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncReturn;

/**
 * Provides asynchronous operations for erasure coding in HDFS Federation.
 * This class extends {@link org.apache.hadoop.hdfs.server.federation.router.ErasureCoding}
 * and overrides its methods to perform erasure coding operations in a non-blocking manner,
 * allowing for concurrent execution and improved performance.
 */
public class AsyncErasureCoding extends ErasureCoding {
  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;

  public AsyncErasureCoding(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient =  this.rpcServer.getRPCClient();
    this.namenodeResolver = this.rpcClient.getNamenodeResolver();
  }

  /**
   * Asynchronously get an array of all erasure coding policies.
   * This method checks the operation category and then invokes the
   * getErasureCodingPolicies method concurrently across all namespaces.
   * <p>
   * The results are merged and returned as an array of ErasureCodingPolicyInfo.
   *
   * @return Array of ErasureCodingPolicyInfo.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getErasureCodingPolicies");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();

    rpcClient.invokeConcurrent(
        nss, method, true, false, ErasureCodingPolicyInfo[].class);
    asyncApply(
        (ApplyFunction<Map<FederationNamespaceInfo, ErasureCodingPolicyInfo[]>,
            ErasureCodingPolicyInfo[]>) ret -> merge(ret, ErasureCodingPolicyInfo.class));

    return asyncReturn(ErasureCodingPolicyInfo[].class);
  }

  /**
   * Asynchronously get the erasure coding codecs available.
   * This method checks the operation category and then invokes the
   * getErasureCodingCodecs method concurrently across all namespaces.
   * <p>
   * The results are merged into a single map of codec names to codec properties.
   *
   * @return Map of erasure coding codecs.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public Map<String, String> getErasureCodingCodecs() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getErasureCodingCodecs");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();

    rpcClient.invokeConcurrent(
        nss, method, true, false, Map.class);

    asyncApply((ApplyFunction<Map<FederationNamespaceInfo,
        Map<String, String>>, Map<String, String>>) retCodecs -> {
        Map<String, String> ret = new HashMap<>();
        Object obj = retCodecs;
        @SuppressWarnings("unchecked")
        Map<FederationNamespaceInfo, Map<String, String>> results =
            (Map<FederationNamespaceInfo, Map<String, String>>)obj;
        Collection<Map<String, String>> allCodecs = results.values();
        for (Map<String, String> codecs : allCodecs) {
          ret.putAll(codecs);
        }
        return ret;
      });

    return asyncReturn(Map.class);
  }

  /**
   * Asynchronously add an array of erasure coding policies.
   * This method checks the operation category and then invokes the
   * addErasureCodingPolicies method concurrently across all namespaces.
   * <p>
   * The results are merged and returned as an array of AddErasureCodingPolicyResponse.
   *
   * @param policies Array of erasure coding policies to add.
   * @return Array of AddErasureCodingPolicyResponse.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("addErasureCodingPolicies",
        new Class<?>[] {ErasureCodingPolicy[].class}, new Object[] {policies});
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();

    rpcClient.invokeConcurrent(
        nss, method, true, false, AddErasureCodingPolicyResponse[].class);

    asyncApply(
        (ApplyFunction<Map<FederationNamespaceInfo, AddErasureCodingPolicyResponse[]>,
            AddErasureCodingPolicyResponse[]>) ret -> {
          return merge(ret, AddErasureCodingPolicyResponse.class);
        });
    return asyncReturn(AddErasureCodingPolicyResponse[].class);
  }

  /**
   * Asynchronously get the erasure coding policy for a given source path.
   * This method checks the operation category and then invokes the
   * getErasureCodingPolicy method sequentially for the given path.
   * <p>
   * The result is returned as an ErasureCodingPolicy object.
   *
   * @param src Source path to get the erasure coding policy for.
   * @return ErasureCodingPolicy for the given path.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(src, false, false);
    RemoteMethod remoteMethod = new RemoteMethod("getErasureCodingPolicy",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeSequential(
        locations, remoteMethod, null, null);

    asyncApply(ret -> {
      return (ErasureCodingPolicy) ret;
    });

    return asyncReturn(ErasureCodingPolicy.class);
  }

  /**
   * Asynchronously get the EC topology result for the given policies.
   * This method checks the operation category and then invokes the
   * getECTopologyResultForPolicies method concurrently across all namespaces.
   * <p>
   * The results are merged and the first unsupported result is returned.
   *
   * @param policyNames Array of policy names to check.
   * @return ECTopologyVerifierResult for the policies.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      String[] policyNames) throws IOException {
    RemoteMethod method = new RemoteMethod("getECTopologyResultForPolicies",
        new Class<?>[] {String[].class}, new Object[] {policyNames});
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    if (nss.isEmpty()) {
      throw new IOException("No namespace availaible.");
    }

    rpcClient.invokeConcurrent(nss, method, true, false,
        ECTopologyVerifierResult.class);
    asyncApply((ApplyFunction<Map<FederationNamespaceInfo, ECTopologyVerifierResult>,
        ECTopologyVerifierResult>) ret -> {
        for (Map.Entry<FederationNamespaceInfo, ECTopologyVerifierResult> entry :
            ret.entrySet()) {
            if (!entry.getValue().isSupported()) {
              return entry.getValue();
            }
          }
        // If no negative result, return the result from the first namespace.
        return ret.get(nss.iterator().next());
      });
    return asyncReturn(ECTopologyVerifierResult.class);
  }

  /**
   * Asynchronously get the erasure coding block group statistics.
   * This method checks the operation category and then invokes the
   * getECBlockGroupStats method concurrently across all namespaces.
   * <p>
   * The results are merged and returned as an ECBlockGroupStats object.
   *
   * @return ECBlockGroupStats for the erasure coding block groups.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getECBlockGroupStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(
        nss, method, true, false, ECBlockGroupStats.class);

    asyncApply((ApplyFunction<Map<FederationNamespaceInfo, ECBlockGroupStats>,
        ECBlockGroupStats>) allStats -> {
        return ECBlockGroupStats.merge(allStats.values());
      });
    return asyncReturn(ECBlockGroupStats.class);
  }
}