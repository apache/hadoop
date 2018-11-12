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
package org.apache.hadoop.yarn.server.resourcemanager.volume.csi;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.VolumeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningResults;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningTask;
import org.apache.hadoop.yarn.server.volume.csi.CsiAdaptorClientProtocol;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A service manages all volumes.
 */
public class VolumeManagerImpl extends AbstractService
    implements VolumeManager {

  private static final Log LOG = LogFactory.getLog(VolumeManagerImpl.class);

  private final VolumeStates volumeStates;
  private ScheduledExecutorService provisioningExecutor;
  private CsiAdaptorClientProtocol adaptorClient;

  private final static int PROVISIONING_TASK_THREAD_POOL_SIZE = 10;

  public VolumeManagerImpl() {
    super(VolumeManagerImpl.class.getName());
    this.volumeStates = new VolumeStates();
    this.provisioningExecutor = Executors
        .newScheduledThreadPool(PROVISIONING_TASK_THREAD_POOL_SIZE);
    this.adaptorClient = new CsiAdaptorClient();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    provisioningExecutor.shutdown();
    super.serviceStop();
  }

  @Override
  public VolumeStates getVolumeStates() {
    return this.volumeStates;
  }

  @Override
  public Volume addOrGetVolume(Volume volume) {
    if (volumeStates.getVolume(volume.getVolumeId()) != null) {
      // volume already exists
      return volumeStates.getVolume(volume.getVolumeId());
    } else {
      // add the volume and set the client
      ((VolumeImpl) volume).setClient(adaptorClient);
      this.volumeStates.addVolumeIfAbsent(volume);
      return volume;
    }
  }

  @VisibleForTesting
  public void setClient(CsiAdaptorClientProtocol client) {
    this.adaptorClient = client;
  }

  @Override
  public ScheduledFuture<VolumeProvisioningResults> schedule(
      VolumeProvisioningTask volumeProvisioningTask,
      int delaySecond) {
    LOG.info("Scheduling provision volume task (with delay "
        + delaySecond + "s)," + " handling "
        + volumeProvisioningTask.getVolumes().size()
        + " volume provisioning");
    return provisioningExecutor.schedule(volumeProvisioningTask,
        delaySecond, TimeUnit.SECONDS);
  }
}
