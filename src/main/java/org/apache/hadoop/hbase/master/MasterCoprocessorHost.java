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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.*;

import java.io.IOException;

/**
 * Provides the coprocessor framework and environment for master oriented
 * operations.  {@link HMaster} interacts with the loaded coprocessors
 * through this class.
 */
public class MasterCoprocessorHost
    extends CoprocessorHost<MasterCoprocessorHost.MasterEnvironment> {

  /**
   * Coprocessor environment extension providing access to master related
   * services.
   */
  static class MasterEnvironment extends CoprocessorHost.Environment
      implements MasterCoprocessorEnvironment {
    private MasterServices masterServices;

    public MasterEnvironment(Class<?> implClass, Coprocessor impl,
        Coprocessor.Priority priority, MasterServices services) {
      super(impl, priority);
      this.masterServices = services;
    }

    public MasterServices getMasterServices() {
      return masterServices;
    }
  }

  private MasterServices masterServices;

  MasterCoprocessorHost(final MasterServices services, final Configuration conf) {
    this.masterServices = services;

    loadSystemCoprocessors(conf, MASTER_COPROCESSOR_CONF_KEY);
  }

  @Override
  public MasterEnvironment createEnvironment(Class<?> implClass,
      Coprocessor instance, Coprocessor.Priority priority) {
    return new MasterEnvironment(implClass, instance, priority, masterServices);
  }

  /* Implementation of hooks for invoking MasterObservers */
  void preCreateTable(HTableDescriptor desc, byte[][] splitKeys)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preCreateTable(env, desc, splitKeys);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postCreateTable(HRegionInfo[] regions, boolean sync) throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postCreateTable(env, regions, sync);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preDeleteTable(byte[] tableName) throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preDeleteTable(env, tableName);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postDeleteTable(byte[] tableName) throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postDeleteTable(env, tableName);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preModifyTable(final byte[] tableName, HTableDescriptor htd)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preModifyTable(env, tableName, htd);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postModifyTable(final byte[] tableName, HTableDescriptor htd)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postModifyTable(env, tableName, htd);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preAddColumn(byte [] tableName, HColumnDescriptor column)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preAddColumn(env, tableName, column);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postAddColumn(byte [] tableName, HColumnDescriptor column)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postAddColumn(env, tableName, column);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preModifyColumn(byte [] tableName, HColumnDescriptor descriptor)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preModifyColumn(
            env, tableName, descriptor);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postModifyColumn(byte [] tableName, HColumnDescriptor descriptor)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postModifyColumn(
            env, tableName, descriptor);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preDeleteColumn(final byte [] tableName, final byte [] c)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preDeleteColumn(env, tableName, c);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postDeleteColumn(final byte [] tableName, final byte [] c)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postDeleteColumn(env, tableName, c);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preEnableTable(final byte [] tableName) throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preEnableTable(env, tableName);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postEnableTable(final byte [] tableName) throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postEnableTable(env, tableName);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preDisableTable(final byte [] tableName) throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preDisableTable(env, tableName);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postDisableTable(final byte [] tableName) throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postDisableTable(env, tableName);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preMove(final HRegionInfo region, final HServerInfo srcServer, final HServerInfo destServer)
      throws UnknownRegionException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preMove(
            env, region, srcServer, destServer);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void postMove(final HRegionInfo region, final HServerInfo srcServer, final HServerInfo destServer)
      throws UnknownRegionException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postMove(
            env, region, srcServer, destServer);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  boolean preAssign(final byte [] regionName, final boolean force)
      throws IOException {
    boolean bypass = false;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preAssign(env, regionName, force);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  void postAssign(final HRegionInfo regionInfo) throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postAssign(env, regionInfo);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  boolean preUnassign(final byte [] regionName, final boolean force)
      throws IOException {
    boolean bypass = false;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preUnassign(
            env, regionName, force);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  void postUnassign(final HRegionInfo regionInfo, final boolean force)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postUnassign(
            env, regionInfo, force);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  boolean preBalance() throws IOException {
    boolean bypass = false;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preBalance(env);
        bypass |= env.shouldBypass();
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return bypass;
  }

  void postBalance() throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postBalance(env);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  boolean preBalanceSwitch(final boolean b) throws IOException {
    boolean balance = b;
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        balance = ((MasterObserver)env.getInstance()).preBalanceSwitch(
            env, balance);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
    return balance;
  }

  void postBalanceSwitch(final boolean oldValue, final boolean newValue)
      throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).postBalanceSwitch(
            env, oldValue, newValue);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preShutdown() throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preShutdown(env);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

  void preStopMaster() throws IOException {
    for (MasterEnvironment env: coprocessors) {
      if (env.getInstance() instanceof MasterObserver) {
        ((MasterObserver)env.getInstance()).preStopMaster(env);
        if (env.shouldComplete()) {
          break;
        }
      }
    }
  }

}
