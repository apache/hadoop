package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.FederatedNamespaceIds;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.ipc.NameServiceStateIdMode;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class RouterStateIdCache {

  private static final Map<String, FederatedNamespaceIds> stateIdCachByNs = new ConcurrentHashMap();
  private static Cache<UniqueCallID, FederatedNamespaceIds> stateIdCacheByCallId;

  static {
    HdfsConfiguration conf = new HdfsConfiguration();
    long timeout = conf.getTimeDuration(RBFConfigKeys.DFS_ROUTER_CACHE_STATE_ID_TIMEOUT,
        RBFConfigKeys.DFS_ROUTER_CACHE_STATE_ID_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    stateIdCacheByCallId =
        CacheBuilder.newBuilder().expireAfterWrite(timeout, TimeUnit.MILLISECONDS).build();
  }

  static FederatedNamespaceIds get(String nsId) {
    stateIdCachByNs.computeIfAbsent(nsId, n -> new FederatedNamespaceIds(NameServiceStateIdMode.PROXY));
    return stateIdCachByNs.get(nsId);
  }

  static FederatedNamespaceIds get(UniqueCallID id) {
    return stateIdCacheByCallId.getIfPresent(id);
  }

  static void put(UniqueCallID uid, FederatedNamespaceIds ids) {
    stateIdCacheByCallId.put(uid, ids);
  }

  static void remove(UniqueCallID uid) {
    stateIdCacheByCallId.invalidate(uid);
  }

  @VisibleForTesting
  static long size() {
    return stateIdCacheByCallId.size() + stateIdCachByNs.size();
  }

  @VisibleForTesting
  static void clear() {
    stateIdCacheByCallId.cleanUp();
    stateIdCachByNs.clear();
  }

  static class UniqueCallID {
    final byte[] clientId;
    final int callId;

    UniqueCallID(byte[] clientId, int callId) {
      this.clientId = clientId;
      this.callId = callId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UniqueCallID that = (UniqueCallID) o;
      return callId == that.callId && Arrays.equals(clientId, that.clientId);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(callId);
      result = 31 * result + Arrays.hashCode(clientId);
      return result;
    }
  }
}
