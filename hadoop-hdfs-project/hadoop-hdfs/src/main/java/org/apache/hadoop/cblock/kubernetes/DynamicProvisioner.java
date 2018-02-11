/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.cblock.kubernetes;

import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.RequestBody;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ISCSIVolumeSource;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ObjectReference;
import io.kubernetes.client.models.V1PersistentVolume;
import io.kubernetes.client.models.V1PersistentVolumeClaim;
import io.kubernetes.client.models.V1PersistentVolumeSpec;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import okio.Buffer;
import org.apache.hadoop.cblock.cli.CBlockCli;
import org.apache.hadoop.cblock.exception.CBlockException;
import org.apache.hadoop.cblock.proto.MountVolumeResponse;
import org.apache.hadoop.cblock.storage.StorageManager;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.ratis.shaded.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ISCSI_ADVERTISED_IP;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ISCSI_ADVERTISED_PORT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_ISCSI_ADVERTISED_PORT_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSI_SERVER_ADDRESS_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSI_SERVER_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_KUBERNETES_CBLOCK_USER;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_KUBERNETES_CBLOCK_USER_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_KUBERNETES_CONFIG_FILE_KEY;

/**
 * Kubernetes Dynamic Persistent Volume provisioner.
 *
 * Listens on the kubernetes feed and creates the appropriate cblock AND
 * kubernetes PersistentVolume according to the created PersistentVolumeClaims.
 */
public class DynamicProvisioner implements Runnable{

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(DynamicProvisioner.class);

  private static final String STORAGE_CLASS = "cblock";

  private static final String PROVISIONER_ID = "hadoop.apache.org/cblock";
  private static final String KUBERNETES_PROVISIONER_KEY =
      "volume.beta.kubernetes.io/storage-provisioner";
  private static final String KUBERNETES_BIND_COMPLETED_KEY =
      "pv.kubernetes.io/bind-completed";

  private boolean running = true;

  private final StorageManager storageManager;

  private String kubernetesConfigFile;

  private String externalIp;

  private int externalPort;

  private String cblockUser;

  private CoreV1Api api;

  private ApiClient client;

  private Thread watcherThread;

  public  DynamicProvisioner(OzoneConfiguration ozoneConf,
      StorageManager storageManager) throws IOException {
    this.storageManager = storageManager;

    kubernetesConfigFile = ozoneConf
        .getTrimmed(DFS_CBLOCK_KUBERNETES_CONFIG_FILE_KEY);

    String jscsiServerAddress = ozoneConf
        .get(DFS_CBLOCK_JSCSI_SERVER_ADDRESS_KEY,
            DFS_CBLOCK_JSCSI_SERVER_ADDRESS_DEFAULT);

    externalIp = ozoneConf.
        getTrimmed(DFS_CBLOCK_ISCSI_ADVERTISED_IP, jscsiServerAddress);

    externalPort = ozoneConf.
        getInt(DFS_CBLOCK_ISCSI_ADVERTISED_PORT,
            DFS_CBLOCK_ISCSI_ADVERTISED_PORT_DEFAULT);

    cblockUser = ozoneConf.getTrimmed(DFS_CBLOCK_KUBERNETES_CBLOCK_USER,
        DFS_CBLOCK_KUBERNETES_CBLOCK_USER_DEFAULT);


  }

  public void init() throws IOException {
    if (kubernetesConfigFile != null) {
      client = Config.fromConfig(kubernetesConfigFile);
    } else {
      client = Config.fromCluster();
    }
    client.getHttpClient().setReadTimeout(60, TimeUnit.SECONDS);
    Configuration.setDefaultApiClient(client);
    api = new CoreV1Api();

    watcherThread = new Thread(this);
    watcherThread.setName("DynamicProvisioner");
    watcherThread.setDaemon(true);
  }

  @Override
  public void run() {
    LOGGER.info("Starting kubernetes dynamic provisioner.");
    while (running) {
      String resourceVersion = null;
      try {

        Watch<V1PersistentVolumeClaim> watch = Watch.createWatch(client,
            api.listPersistentVolumeClaimForAllNamespacesCall(null,
                null,
                false,
                null,
                null,
                null,
                resourceVersion,
                null,
                true,
                null,
                null),
            new TypeToken<Watch.Response<V1PersistentVolumeClaim>>() {
            }.getType());


        //check the new pvc resources, and create cblock + pv if needed
        for (Watch.Response<V1PersistentVolumeClaim> item : watch) {
          V1PersistentVolumeClaim claim = item.object;

          if (isPvMissingForPvc(claim)) {

            LOGGER.info("Provisioning volumes for PVC {}/{}",
                claim.getMetadata().getNamespace(),
                claim.getMetadata().getName());

            if (LOGGER.isDebugEnabled()) {
              RequestBody request =
                  api.getApiClient().serialize(claim, "application/json");

              final Buffer buffer = new Buffer();
              request.writeTo(buffer);
              LOGGER.debug("New PVC is detected: " + buffer.readUtf8());
            }

            String volumeName = createVolumeName(claim);

            long size = CBlockCli.parseSize(
                claim.getSpec().getResources().getRequests().get("storage"));

            createCBlock(volumeName, size);
            createPersistentVolumeFromPVC(item.object, volumeName);
          }
        }
      } catch (Exception ex) {
        if (ex.getCause() != null && ex
            .getCause() instanceof SocketTimeoutException) {
          //This is normal. We are connection to the kubernetes server and the
          //connection should be reopened time to time...
          LOGGER.debug("Time exception occured", ex);
        } else {
          LOGGER.error("Error on provisioning persistent volumes.", ex);
          try {
            //we can try again in the main loop
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOGGER.error("Error on sleeping after an error.", e);
          }
        }
      }
    }
  }

  private boolean isPvMissingForPvc(V1PersistentVolumeClaim claim) {

    Map<String, String> annotations = claim.getMetadata().getAnnotations();

    return claim.getStatus().getPhase().equals("Pending") && STORAGE_CLASS
        .equals(claim.getSpec().getStorageClassName()) && PROVISIONER_ID
        .equals(annotations.get(KUBERNETES_PROVISIONER_KEY)) && !"yes"
        .equals(annotations.get(KUBERNETES_BIND_COMPLETED_KEY));
  }

  @VisibleForTesting
  protected String createVolumeName(V1PersistentVolumeClaim claim) {
    return claim.getMetadata().getName() + "-" + claim.getMetadata()
        .getUid();
  }

  public void stop() {
    running = false;
    try {
      watcherThread.join(60000);
    } catch (InterruptedException e) {
      LOGGER.error("Kubernetes watcher thread can't stopped gracefully.", e);
    }
  }

  private void createCBlock(String volumeName, long size)
      throws CBlockException {

    MountVolumeResponse mountVolumeResponse =
        storageManager.isVolumeValid(cblockUser, volumeName);
    if (!mountVolumeResponse.getIsValid()) {
      storageManager
          .createVolume(cblockUser, volumeName, size, 4 * 1024);
    }
  }

  private void createPersistentVolumeFromPVC(V1PersistentVolumeClaim claim,
      String volumeName) throws ApiException, IOException {

    V1PersistentVolume v1PersistentVolume =
        persitenceVolumeBuilder(claim, volumeName);

    if (LOGGER.isDebugEnabled()) {
      RequestBody request =
          api.getApiClient().serialize(v1PersistentVolume, "application/json");

      final Buffer buffer = new Buffer();
      request.writeTo(buffer);
      LOGGER.debug("Creating new PV: " + buffer.readUtf8());
    }
    api.createPersistentVolume(v1PersistentVolume, null);
  }

  protected V1PersistentVolume persitenceVolumeBuilder(
      V1PersistentVolumeClaim claim,
      String volumeName) {

    V1PersistentVolume v1PersistentVolume = new V1PersistentVolume();
    v1PersistentVolume.setKind("PersistentVolume");
    v1PersistentVolume.setApiVersion("v1");

    V1ObjectMeta metadata = new V1ObjectMeta();
    metadata.setName(volumeName);
    metadata.setNamespace(claim.getMetadata().getNamespace());
    metadata.setAnnotations(new HashMap<>());

    metadata.getAnnotations()
        .put("pv.kubernetes.io/provisioned-by", PROVISIONER_ID);

    metadata.getAnnotations()
        .put("volume.beta.kubernetes.io/storage-class", STORAGE_CLASS);

    v1PersistentVolume.setMetadata(metadata);

    V1PersistentVolumeSpec spec = new V1PersistentVolumeSpec();

    spec.setCapacity(new HashMap<>());
    spec.getCapacity().put("storage",
        claim.getSpec().getResources().getRequests().get("storage"));

    spec.setAccessModes(new ArrayList<>());
    spec.getAccessModes().add("ReadWriteOnce");

    V1ObjectReference claimRef = new V1ObjectReference();
    claimRef.setName(claim.getMetadata().getName());
    claimRef.setNamespace(claim.getMetadata().getNamespace());
    claimRef.setKind(claim.getKind());
    claimRef.setApiVersion(claim.getApiVersion());
    claimRef.setUid(claim.getMetadata().getUid());
    spec.setClaimRef(claimRef);

    spec.persistentVolumeReclaimPolicy("Delete");

    V1ISCSIVolumeSource iscsi = new V1ISCSIVolumeSource();
    iscsi.setIqn(cblockUser + ":" + volumeName);
    iscsi.setLun(0);
    iscsi.setFsType("ext4");
    String portal = externalIp + ":" + externalPort;
    iscsi.setTargetPortal(portal);
    iscsi.setPortals(new ArrayList<>());
    iscsi.getPortals().add(portal);

    spec.iscsi(iscsi);
    v1PersistentVolume.setSpec(spec);
    return v1PersistentVolume;
  }


  @VisibleForTesting
  protected CoreV1Api getApi() {
    return api;
  }

  public void start() {
    watcherThread.start();
  }
}
