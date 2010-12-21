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
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preCreateTable(env, desc, splitKeys);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postCreateTable(HRegionInfo[] regions, boolean sync) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postCreateTable(env, regions, sync);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preDeleteTable(byte[] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preDeleteTable(env, tableName);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postDeleteTable(byte[] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postDeleteTable(env, tableName);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preModifyTable(final byte[] tableName, HTableDescriptor htd)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preModifyTable(env, tableName, htd);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postModifyTable(final byte[] tableName, HTableDescriptor htd)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postModifyTable(env, tableName, htd);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preAddColumn(byte [] tableName, HColumnDescriptor column)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preAddColumn(env, tableName, column);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postAddColumn(byte [] tableName, HColumnDescriptor column)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postAddColumn(env, tableName, column);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preModifyColumn(byte [] tableName, HColumnDescriptor descriptor)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preModifyColumn(
              env, tableName, descriptor);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postModifyColumn(byte [] tableName, HColumnDescriptor descriptor)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postModifyColumn(
              env, tableName, descriptor);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preDeleteColumn(final byte [] tableName, final byte [] c)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preDeleteColumn(env, tableName, c);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postDeleteColumn(final byte [] tableName, final byte [] c)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postDeleteColumn(env, tableName, c);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preEnableTable(final byte [] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preEnableTable(env, tableName);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postEnableTable(final byte [] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postEnableTable(env, tableName);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preDisableTable(final byte [] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preDisableTable(env, tableName);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postDisableTable(final byte [] tableName) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postDisableTable(env, tableName);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preMove(final HRegionInfo region, final HServerInfo srcServer, final HServerInfo destServer)
      throws UnknownRegionException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preMove(
              env, region, srcServer, destServer);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postMove(final HRegionInfo region, final HServerInfo srcServer, final HServerInfo destServer)
      throws UnknownRegionException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postMove(
              env, region, srcServer, destServer);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  boolean preAssign(final byte [] regionName, final boolean force)
      throws IOException {
    boolean bypass = false;
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preAssign(env, regionName, force);
          bypass |= env.shouldBypass();
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return bypass;
  }

  void postAssign(final HRegionInfo regionInfo) throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postAssign(env, regionInfo);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  boolean preUnassign(final byte [] regionName, final boolean force)
      throws IOException {
    boolean bypass = false;
    try {
      coprocessorLock.readLock().lock();
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
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return bypass;
  }

  void postUnassign(final HRegionInfo regionInfo, final boolean force)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postUnassign(
              env, regionInfo, force);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  boolean preBalance() throws IOException {
    try {
      boolean bypass = false;
      coprocessorLock.readLock().lock();
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
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void postBalance() throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postBalance(env);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  boolean preBalanceSwitch(final boolean b) throws IOException {
    boolean balance = b;
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          balance = ((MasterObserver)env.getInstance()).preBalanceSwitch(
              env, balance);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
    return balance;
  }

  void postBalanceSwitch(final boolean oldValue, final boolean newValue)
      throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).postBalanceSwitch(
              env, oldValue, newValue);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preShutdown() throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preShutdown(env);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

  void preStopMaster() throws IOException {
    try {
      coprocessorLock.readLock().lock();
      for (MasterEnvironment env: coprocessors) {
        if (env.getInstance() instanceof MasterObserver) {
          ((MasterObserver)env.getInstance()).preStopMaster(env);
          if (env.shouldComplete()) {
            break;
          }
        }
      }
    } finally {
      coprocessorLock.readLock().unlock();
    }
  }

}
