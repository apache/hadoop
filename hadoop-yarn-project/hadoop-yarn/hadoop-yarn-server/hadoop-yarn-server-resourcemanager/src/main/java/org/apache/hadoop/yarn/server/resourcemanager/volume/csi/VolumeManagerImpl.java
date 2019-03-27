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
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.CsiAdaptorProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetPluginInfoResponse;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.lifecycle.Volume;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningResults;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.provisioner.VolumeProvisioningTask;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A service manages all volumes.
 */
public class VolumeManagerImpl extends AbstractService
    implements VolumeManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(VolumeManagerImpl.class);

  private final VolumeStates volumeStates;
  private ScheduledExecutorService provisioningExecutor;
  private Map<String, CsiAdaptorProtocol> csiAdaptorMap;

  private final static int PROVISIONING_TASK_THREAD_POOL_SIZE = 10;

  public VolumeManagerImpl() {
    super(VolumeManagerImpl.class.getName());
    this.volumeStates = new VolumeStates();
    this.csiAdaptorMap = new ConcurrentHashMap<>();
    this.provisioningExecutor = Executors
        .newScheduledThreadPool(PROVISIONING_TASK_THREAD_POOL_SIZE);
  }

  // Init the CSI adaptor cache according to the configuration.
  // user only needs to configure a list of adaptor addresses,
  // this method extracts each address and init an adaptor client,
  // then proceed with a hand-shake by calling adaptor's getPluginInfo
  // method to retrieve the driver info. If the driver can be resolved,
  // it is then added to the cache. Note, we don't allow two drivers
  // specified with same driver-name even version is different.
  private void initCsiAdaptorCache(
      final Map<String, CsiAdaptorProtocol> adaptorMap, Configuration conf)
      throws IOException, YarnException {
    LOG.info("Initializing cache for csi-driver-adaptors");
    String[] addresses =
        conf.getStrings(YarnConfiguration.NM_CSI_ADAPTOR_ADDRESSES);
    if (addresses != null && addresses.length > 0) {
      for (String addr : addresses) {
        LOG.info("Found csi-driver-adaptor socket address: " + addr);
        InetSocketAddress address = NetUtils.createSocketAddr(addr);
        YarnRPC rpc = YarnRPC.create(conf);
        UserGroupInformation currentUser =
            UserGroupInformation.getCurrentUser();
        CsiAdaptorProtocol adaptorClient = NMProxy
            .createNMProxy(conf, CsiAdaptorProtocol.class, currentUser, rpc,
                address);
        // Attempt to resolve the driver by contacting to
        // the diver's identity service on the given address.
        // If the call failed, the initialization is also failed
        // in order running into inconsistent state.
        LOG.info("Retrieving info from csi-driver-adaptor on address " + addr);
        GetPluginInfoResponse response =
            adaptorClient.getPluginInfo(GetPluginInfoRequest.newInstance());
        if (!Strings.isNullOrEmpty(response.getDriverName())) {
          String driverName = response.getDriverName();
          if (adaptorMap.containsKey(driverName)) {
            throw new YarnException(
                "Duplicate driver adaptor found," + " driver name: "
                    + driverName);
          }
          adaptorMap.put(driverName, adaptorClient);
          LOG.info("CSI Adaptor added to the cache, adaptor name: " + driverName
              + ", driver version: " + response.getVersion());
        }
      }
    }
  }

  /**
   * Returns a CsiAdaptorProtocol client by the given driver name,
   * returns null if no adaptor is found for the driver, that means
   * the driver has not registered to the volume manager yet enhance not valid.
   * @param driverName the name of the driver
   * @return CsiAdaptorProtocol client or null if driver not registered
   */
  public CsiAdaptorProtocol getAdaptorByDriverName(String driverName) {
    return csiAdaptorMap.get(driverName);
  }

  @VisibleForTesting
  @Override
  public void registerCsiDriverAdaptor(String driverName,
      CsiAdaptorProtocol client) {
    this.csiAdaptorMap.put(driverName, client);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    initCsiAdaptorCache(csiAdaptorMap, conf);
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
      this.volumeStates.addVolumeIfAbsent(volume);
      return volume;
    }
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
