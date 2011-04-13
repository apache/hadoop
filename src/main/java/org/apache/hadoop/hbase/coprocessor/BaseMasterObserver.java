/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.UnknownRegionException;

import java.io.IOException;

public class BaseMasterObserver implements MasterObserver {
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> env,
      HTableDescriptor desc, byte[][] splitKeys) throws IOException {
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> env,
      HRegionInfo[] regions, boolean sync) throws IOException {
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName) throws IOException {
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName) throws IOException {
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName, HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName, HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName, HColumnDescriptor column) throws IOException {
  }

  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName, HColumnDescriptor column) throws IOException {
  }

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName, byte[] c) throws IOException {
  }

  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName, byte[] c) throws IOException {
  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName) throws IOException {
  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName) throws IOException {
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName) throws IOException {
  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] tableName) throws IOException {
  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> env,
      HRegionInfo region, HServerInfo srcServer, HServerInfo destServer)
  throws UnknownRegionException {
  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> env,
      HRegionInfo region, HServerInfo srcServer, HServerInfo destServer)
  throws UnknownRegionException {
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] regionName, boolean force) throws IOException {
  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> env,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> env,
      byte[] regionName, boolean force) throws IOException {
  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> env,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> env)
      throws IOException {
  }

  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> env)
      throws IOException {
  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> env,
      boolean b) throws IOException {
    return b;
  }

  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> env,
      boolean oldValue, boolean newValue) throws IOException {
  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> env)
      throws IOException {
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> env)
      throws IOException {
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }
}
