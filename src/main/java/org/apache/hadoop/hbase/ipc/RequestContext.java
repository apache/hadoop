/*
 * Copyright The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.security.User;

import java.net.InetAddress;

/**
 * Represents client information (authenticated username, remote address, protocol)
 * for the currently executing request within a RPC server handler thread.  If
 * called outside the context of a RPC request, all values will be
 * <code>null</code>.
 */
public class RequestContext {
  private static ThreadLocal<RequestContext> instance =
      new ThreadLocal<RequestContext>() {
        protected RequestContext initialValue() {
          return new RequestContext(null, null, null);
        }
      };

  public static RequestContext get() {
    return instance.get();
  }


  /**
   * Returns the user credentials associated with the current RPC request or
   * <code>null</code> if no credentials were provided.
   * @return
   */
  public static User getRequestUser() {
    RequestContext ctx = instance.get();
    if (ctx != null) {
      return ctx.getUser();
    }
    return null;
  }

  /**
   * Returns the username for any user associated with the current RPC
   * request or <code>null</code> if no user is set.
   */
  public static String getRequestUserName() {
    User user = getRequestUser();
    if (user != null) {
      return user.getShortName();
    }
    return null;
  }

  /**
   * Indicates whether or not the current thread is within scope of executing
   * an RPC request.
   */
  public static boolean isInRequestContext() {
    RequestContext ctx = instance.get();
    if (ctx != null) {
      return ctx.isInRequest();
    }
    return false;
  }

  /**
   * Initializes the client credentials for the current request.
   * @param user
   * @param remoteAddress
   * @param protocol
   */
  public static void set(User user,
      InetAddress remoteAddress,
      Class<? extends VersionedProtocol> protocol) {
    RequestContext ctx = instance.get();
    ctx.user = user;
    ctx.remoteAddress = remoteAddress;
    ctx.protocol = protocol;
    ctx.inRequest = true;
  }

  /**
   * Clears out the client credentials for a given request.
   */
  public static void clear() {
    RequestContext ctx = instance.get();
    ctx.user = null;
    ctx.remoteAddress = null;
    ctx.protocol = null;
    ctx.inRequest = false;
  }

  private User user;
  private InetAddress remoteAddress;
  private Class<? extends VersionedProtocol> protocol;
  // indicates we're within a RPC request invocation
  private boolean inRequest;

  private RequestContext(User user, InetAddress remoteAddr,
      Class<? extends VersionedProtocol> protocol) {
    this.user = user;
    this.remoteAddress = remoteAddr;
    this.protocol = protocol;
  }

  public User getUser() {
    return user;
  }

  public InetAddress getRemoteAddress() {
    return remoteAddress;
  }

  public Class<? extends VersionedProtocol> getProtocol() {
    return protocol;
  }

  public boolean isInRequest() {
    return inRequest;
  }
}
