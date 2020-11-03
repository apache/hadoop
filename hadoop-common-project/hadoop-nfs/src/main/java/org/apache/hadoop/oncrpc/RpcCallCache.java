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
package org.apache.hadoop.oncrpc;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * This class is used for handling the duplicate <em>non-idempotenty</em> Rpc
 * calls. A non-idempotent request is processed as follows:
 * <ul>
 * <li>If the request is being processed for the first time, its state is
 * in-progress in cache.</li>
 * <li>If the request is retransimitted and is in-progress state, it is ignored.
 * </li>
 * <li>If the request is retransimitted and is completed, the previous response
 * from the cache is sent back to the client.</li>
 * </ul>
 * <br>
 * A request is identified by the client ID (address of the client) and
 * transaction ID (xid) from the Rpc call.
 * 
 */
public class RpcCallCache {
  
  public static class CacheEntry {
    private RpcResponse response; // null if no response has been sent
    
    public CacheEntry() {
      response = null;
    }
      
    public boolean isInProgress() {
      return response == null;
    }
    
    public boolean isCompleted() {
      return response != null;
    }
    
    public RpcResponse getResponse() {
      return response;
    }
    
    public void setResponse(RpcResponse response) {
      this.response = response;
    }
  }
  
  /**
   * Call that is used to track a client in the {@link RpcCallCache}
   */
  public static class ClientRequest {
    protected final InetAddress clientId;
    protected final int xid;

    public InetAddress getClientId() {
      return clientId;
    }

    public ClientRequest(InetAddress clientId, int xid) {
      this.clientId = clientId;
      this.xid = xid;
    }

    @Override
    public int hashCode() {
	  return xid + clientId.hashCode() * 31;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || !(obj instanceof ClientRequest)) {
        return false;
      }
      ClientRequest other = (ClientRequest) obj;
      return clientId.equals(other.clientId) && (xid == other.xid);
    }
  }
  
  private final String program;
  
  private final Map<ClientRequest, CacheEntry> map;
  
  public RpcCallCache(final String program, final int maxEntries) {
    if (maxEntries <= 0) {
      throw new IllegalArgumentException("Cache size is " + maxEntries
          + ". Should be > 0");
    }
    this.program = program;
    map = new LinkedHashMap<ClientRequest, CacheEntry>() {
      private static final long serialVersionUID = 1L;

      @Override
      protected boolean removeEldestEntry(
          java.util.Map.Entry<ClientRequest, CacheEntry> eldest) {
        return RpcCallCache.this.size() > maxEntries;
      }
    };
  }
  
  /**
   * Return the program name.
   * @return RPC program name
   */
  public String getProgram() {
    return program;
  }

  /**
   * Mark a request as completed and add corresponding response to the cache.
   * @param clientId client IP address
   * @param xid transaction id
   * @param response RPC response
   */
  public void callCompleted(InetAddress clientId, int xid, RpcResponse response) {
    ClientRequest req = new ClientRequest(clientId, xid);
    CacheEntry e;
    synchronized(map) {
      e = map.get(req);
    }
    e.response = response;
  }
  
  /**
   * Check the cache for an entry. If it does not exist, add the request
   * as in progress.
   * @param clientId client IP address
   * @param xid transaction id
   * @return cached entry
   */
  public CacheEntry checkOrAddToCache(InetAddress clientId, int xid) {
    ClientRequest req = new ClientRequest(clientId, xid);
    CacheEntry e;
    synchronized(map) {
      e = map.get(req);
      if (e == null) {
        // Add an inprogress cache entry
        map.put(req, new CacheEntry());
      }
    }
    return e;
  }
  
  /**
   * Return number of cached entries.
   * @return cache size
   */
  public int size() {
    return map.size();
  }
  
  /** 
   * Iterator to the cache entries.
   * @return iterator cache iterator
   */
  @VisibleForTesting
  public Iterator<Entry<ClientRequest, CacheEntry>> iterator() {
    return map.entrySet().iterator();
  }
}
