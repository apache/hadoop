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
package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerPacker;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A naive implementation of the replication source which creates a tar file
 * on-demand without pre-create the compressed archives.
 */
public class OnDemandContainerReplicationSource
    implements ContainerReplicationSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerReplicationSource.class);

  private ContainerController controller;

  private ContainerPacker packer = new TarContainerPacker();

  public OnDemandContainerReplicationSource(
      ContainerController controller) {
    this.controller = controller;
  }

  @Override
  public void prepare(long containerId) {

  }

  @Override
  public void copyData(long containerId, OutputStream destination)
      throws IOException {

    Container container = controller.getContainer(containerId);

    Preconditions
        .checkNotNull(container, "Container is not found " + containerId);

    switch (container.getContainerType()) {
    case KeyValueContainer:
      packer.pack(container,
          destination);
      break;
    default:
      LOG.warn("Container type " + container.getContainerType()
          + " is not replicable as no compression algorithm for that.");
    }

  }
}
