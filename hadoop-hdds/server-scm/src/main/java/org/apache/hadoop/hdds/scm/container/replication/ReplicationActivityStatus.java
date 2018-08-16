/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import javax.management.ObjectName;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.metrics2.util.MBeans;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event listener to track the current state of replication.
 */
public class ReplicationActivityStatus
    implements EventHandler<Boolean>, ReplicationActivityStatusMXBean,
    Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationActivityStatus.class);

  private AtomicBoolean replicationEnabled = new AtomicBoolean();

  private ObjectName jmxObjectName;

  public boolean isReplicationEnabled() {
    return replicationEnabled.get();
  }

  @VisibleForTesting
  public void setReplicationEnabled(boolean enabled) {
    replicationEnabled.set(enabled);
  }

  @VisibleForTesting
  public void enableReplication() {
    replicationEnabled.set(true);
  }

  /**
   * The replication status could be set by async events.
   */
  @Override
  public void onMessage(Boolean enabled, EventPublisher publisher) {
    replicationEnabled.set(enabled);
  }

  public void start() {
    try {
      this.jmxObjectName =
          MBeans.register(
              "StorageContainerManager", "ReplicationActivityStatus", this);
    } catch (Exception ex) {
      LOG.error("JMX bean for ReplicationActivityStatus can't be registered",
          ex);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.jmxObjectName != null) {
      MBeans.unregister(jmxObjectName);
    }
  }
}
