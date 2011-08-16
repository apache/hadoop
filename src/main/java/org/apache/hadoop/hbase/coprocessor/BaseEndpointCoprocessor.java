/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * This abstract class provides default implementation of an Endpoint.
 * It also maintains a CoprocessorEnvironment object which can be
 * used to access region resource.
 *
 * It's recommended to use this abstract class to implement your Endpoint.
 * However you still can just implement the interface CoprocessorProtocol
 * and Coprocessor to develop an Endpoint. But you won't be able to access
 * the region related resource, i.e., CoprocessorEnvironment.
 */
public abstract class BaseEndpointCoprocessor implements Coprocessor,
    CoprocessorProtocol, VersionedProtocol {
  /**
   * This Interfaces' version. Version changes when the Interface changes.
   */
  // All HBase Interfaces used derive from HBaseRPCProtocolVersion.  It
  // maintained a single global version number on all HBase Interfaces.  This
  // meant all HBase RPC was broke though only one of the three RPC Interfaces
  // had changed.  This has since been undone.
  public static final long VERSION = 28L;

  private CoprocessorEnvironment env;

  /**
   * @return env Coprocessor environment.
   */
  public CoprocessorEnvironment getEnvironment() {
    return env;
  }

  @Override
  public void start(CoprocessorEnvironment env) {
    this.env = env;
  }

  @Override
  public void stop(CoprocessorEnvironment env) { }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
  throws IOException {
    return VERSION;
  }
}
