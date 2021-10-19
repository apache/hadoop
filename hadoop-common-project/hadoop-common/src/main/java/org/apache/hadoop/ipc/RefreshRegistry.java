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
package org.apache.hadoop.ipc;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Multimap;

import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to registry custom methods to refresh at runtime.
 * Each identifier maps to one or more RefreshHandlers.
 */
@InterfaceStability.Unstable
public class RefreshRegistry {
  public static final Logger LOG =
      LoggerFactory.getLogger(RefreshRegistry.class);

  // Used to hold singleton instance
  private static class RegistryHolder {
    @SuppressWarnings("All")
    public static RefreshRegistry registry = new RefreshRegistry();
  }

  // Singleton access
  public static RefreshRegistry defaultRegistry() {
    return RegistryHolder.registry;
  }

  private final Multimap<String, RefreshHandler> handlerTable;

  public RefreshRegistry() {
    handlerTable = HashMultimap.create();
  }

  /**
   * Registers an object as a handler for a given identity.
   * Note: will prevent handler from being GC'd, object should unregister itself
   *  when done
   * @param identifier a unique identifier for this resource,
   *                   such as org.apache.hadoop.blacklist
   * @param handler the object to register
   */
  public synchronized void register(String identifier, RefreshHandler handler) {
    if (identifier == null) {
      throw new NullPointerException("Identifier cannot be null");
    }
    handlerTable.put(identifier, handler);
  }

  /**
   * Remove the registered object for a given identity.
   * @param identifier the resource to unregister
   * @return the true if removed
   */
  public synchronized boolean unregister(String identifier, RefreshHandler handler) {
    return handlerTable.remove(identifier, handler);
  }

  public synchronized void unregisterAll(String identifier) {
    handlerTable.removeAll(identifier);
  }

  /**
   * Lookup the responsible handler and return its result.
   * This should be called by the RPC server when it gets a refresh request.
   * @param identifier the resource to refresh
   * @param args the arguments to pass on, not including the program name
   * @throws IllegalArgumentException on invalid identifier
   * @return the response from the appropriate handler
   */
  public synchronized Collection<RefreshResponse> dispatch(String identifier, String[] args) {
    Collection<RefreshHandler> handlers = handlerTable.get(identifier);

    if (handlers.size() == 0) {
      String msg = "Identifier '" + identifier +
        "' does not exist in RefreshRegistry. Valid options are: " +
        Joiner.on(", ").join(handlerTable.keySet());

      throw new IllegalArgumentException(msg);
    }

    ArrayList<RefreshResponse> responses =
      new ArrayList<RefreshResponse>(handlers.size());

    // Dispatch to each handler and store response
    for(RefreshHandler handler : handlers) {
      RefreshResponse response;

      // Run the handler
      try {
        response = handler.handleRefresh(identifier, args);
        if (response == null) {
          throw new NullPointerException("Handler returned null.");
        }

        LOG.info(handlerName(handler) + " responds to '" + identifier +
          "', says: '" + response.getMessage() + "', returns " +
          response.getReturnCode());
      } catch (Exception e) {
        response = new RefreshResponse(-1, e.getLocalizedMessage());
      }

      response.setSenderName(handlerName(handler));
      responses.add(response);
    }

    return responses;
  }

  private String handlerName(RefreshHandler h) {
    return h.getClass().getName() + '@' + Integer.toHexString(h.hashCode());
  }
}
