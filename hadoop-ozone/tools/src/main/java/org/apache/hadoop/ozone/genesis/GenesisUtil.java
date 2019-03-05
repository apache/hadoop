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
package org.apache.hadoop.ozone.genesis;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_PIPELINE_DB;

/**
 * Utility class for benchmark test cases.
 */
public final class GenesisUtil {

  private GenesisUtil() {
    // private constructor.
  }

  public static final String DEFAULT_TYPE = "default";
  public static final String CACHE_10MB_TYPE = "Cache10MB";
  public static final String CACHE_1GB_TYPE = "Cache1GB";
  public static final String CLOSED_TYPE = "ClosedContainer";

  private static final int DB_FILE_LEN = 7;
  private static final String TMP_DIR = "java.io.tmpdir";

  public static Path getTempPath() {
    return Paths.get(System.getProperty(TMP_DIR));
  }

  public static MetadataStore getMetadataStore(String dbType)
      throws IOException {
    Configuration conf = new Configuration();
    MetadataStoreBuilder builder = MetadataStoreBuilder.newBuilder();
    builder.setConf(conf);
    builder.setCreateIfMissing(true);
    builder.setDbFile(
        getTempPath().resolve(RandomStringUtils.randomNumeric(DB_FILE_LEN))
            .toFile());
    switch (dbType) {
    case DEFAULT_TYPE:
      break;
    case CLOSED_TYPE:
      break;
    case CACHE_10MB_TYPE:
      builder.setCacheSize((long) StorageUnit.MB.toBytes(10));
      break;
    case CACHE_1GB_TYPE:
      builder.setCacheSize((long) StorageUnit.GB.toBytes(1));
      break;
    default:
      throw new IllegalStateException("Unknown type: " + dbType);
    }
    return builder.build();
  }

  public static DatanodeDetails createDatanodeDetails(String uuid) {
    Random random = new Random();
    String ipAddress =
        random.nextInt(256) + "." + random.nextInt(256) + "." + random
            .nextInt(256) + "." + random.nextInt(256);

    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(uuid)
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  static StorageContainerManager getScm(OzoneConfiguration conf,
      SCMConfigurator configurator) throws IOException,
      AuthenticationException {
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    if(scmStore.getState() != Storage.StorageState.INITIALIZED) {
      String clusterId = UUID.randomUUID().toString();
      String scmId = UUID.randomUUID().toString();
      scmStore.setClusterId(clusterId);
      scmStore.setScmId(scmId);
      // writes the version file properties
      scmStore.initialize();
    }
    return new StorageContainerManager(conf, configurator);
  }

  static void configureSCM(Configuration conf, int numHandlers) {
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    conf.setInt(ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY, numHandlers);
  }

  static void addPipelines(HddsProtos.ReplicationFactor factor,
      int numPipelines, Configuration conf) throws IOException {
    final File metaDir = ServerUtils.getScmDbDir(conf);
    final File pipelineDBPath = new File(metaDir, SCM_PIPELINE_DB);
    int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
    MetadataStore pipelineStore =
        MetadataStoreBuilder.newBuilder().setCreateIfMissing(true)
            .setConf(conf).setDbFile(pipelineDBPath)
            .setCacheSize(cacheSize * OzoneConsts.MB).build();

    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < factor.getNumber(); i++) {
      nodes
          .add(GenesisUtil.createDatanodeDetails(UUID.randomUUID().toString()));
    }
    for (int i = 0; i < numPipelines; i++) {
      Pipeline pipeline =
          Pipeline.newBuilder()
              .setState(Pipeline.PipelineState.OPEN)
              .setId(PipelineID.randomId())
              .setType(HddsProtos.ReplicationType.RATIS)
              .setFactor(factor)
              .setNodes(nodes)
              .build();
      pipelineStore.put(pipeline.getId().getProtobuf().toByteArray(),
          pipeline.getProtobufMessage().toByteArray());
    }

    pipelineStore.close();
  }

  static OzoneManager getOm(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    OMStorage omStorage = new OMStorage(conf);
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    if (omStorage.getState() != Storage.StorageState.INITIALIZED) {
      omStorage.setClusterId(scmStore.getClusterID());
      omStorage.setScmId(scmStore.getScmId());
      omStorage.setOmId(UUID.randomUUID().toString());
      omStorage.initialize();
    }
    return OzoneManager.createOm(null, conf);
  }

  static void configureOM(Configuration conf, int numHandlers) {
    conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, numHandlers);
  }
}
