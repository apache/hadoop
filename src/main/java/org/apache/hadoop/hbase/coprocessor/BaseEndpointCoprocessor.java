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

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.regionserver.HRegion;

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
    CoprocessorProtocol {
  private CoprocessorEnvironment env;

  /**
   * @param e Coprocessor environment.
   */
  private void setEnvironment(CoprocessorEnvironment e) {
    env = e;
  }

  /**
   * @return env Coprocessor environment.
   */
  public CoprocessorEnvironment getEnvironment() {
    return env;
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return HBaseRPCProtocolVersion.versionID;
  }

  @Override
  public void preOpen(CoprocessorEnvironment e) { }

  /**
   * It initializes the coprocessor resources. If you need to override this
   * method, please remember to call super(e).
   */
  @Override
  public void postOpen(CoprocessorEnvironment e) {
    setEnvironment(e);
  }

  @Override
  public void preClose(CoprocessorEnvironment e, boolean abortRequested) { }

  @Override
  public void postClose(CoprocessorEnvironment e, boolean abortRequested) { }

  @Override
  public void preFlush(CoprocessorEnvironment e) { }

  @Override
  public void postFlush(CoprocessorEnvironment e) { }

  @Override
  public void preCompact(CoprocessorEnvironment e, boolean willSplit) { }

  @Override
  public void postCompact(CoprocessorEnvironment e, boolean willSplit) { }

  @Override
  public void preSplit(CoprocessorEnvironment e) { }

  @Override
  public void postSplit(CoprocessorEnvironment e, HRegion l, HRegion r) { }
}
