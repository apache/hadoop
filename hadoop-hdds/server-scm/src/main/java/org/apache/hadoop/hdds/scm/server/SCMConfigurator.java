/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.server;


import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.security.x509.certificate.authority
    .CertificateServer;

/**
 * This class acts as an SCM builder Class. This class is important for us
 * from a resilience perspective of SCM. This class will allow us swap out
 * different managers and replace with out on manager in the testing phase.
 * <p>
 * At some point in the future, we will make all these managers dynamically
 * loadable, so other developers can extend SCM by replacing various managers.
 * <p>
 * TODO: Add different config keys, so that we can load different managers at
 * run time. This will make it easy to extend SCM without having to replace
 * whole SCM each time.
 * <p>
 * Different Managers supported by this builder are:
 * NodeManager scmNodeManager;
 * PipelineManager pipelineManager;
 * ContainerManager containerManager;
 * BlockManager scmBlockManager;
 * ReplicationManager replicationManager;
 * SCMSafeModeManager scmSafeModeManager;
 * CertificateServer certificateServer;
 * SCMMetadata scmMetadataStore.
 *
 * If any of these are *not* specified then the default version of these
 * managers are used by SCM.
 *
 */
public final class SCMConfigurator {
  private NodeManager scmNodeManager;
  private PipelineManager pipelineManager;
  private ContainerManager containerManager;
  private BlockManager scmBlockManager;
  private ReplicationManager replicationManager;
  private SCMSafeModeManager scmSafeModeManager;
  private CertificateServer certificateServer;
  private SCMMetadataStore metadataStore;

  /**
   * Allows user to specify a version of Node manager to use with this SCM.
   * @param scmNodeManager - Node Manager.
   */
  public void setScmNodeManager(NodeManager scmNodeManager) {
    this.scmNodeManager = scmNodeManager;
  }

  /**
   * Allows user to specify a custom version of PipelineManager to use with
   * this SCM.
   * @param pipelineManager - Pipeline Manager.
   */
  public void setPipelineManager(PipelineManager pipelineManager) {
    this.pipelineManager = pipelineManager;
  }

  /**
   *  Allows user to specify a custom version of containerManager to use with
   *  this SCM.
   * @param containerManager - Container Manager.
   */
  public void setContainerManager(ContainerManager containerManager) {
    this.containerManager = containerManager;
  }

  /**
   *  Allows user to specify a custom version of Block Manager to use with
   *  this SCM.
   * @param scmBlockManager - Block Manager
   */
  public void setScmBlockManager(BlockManager scmBlockManager) {
    this.scmBlockManager = scmBlockManager;
  }

  /**
   * Allows user to specify a custom version of Replication Manager to use
   * with this SCM.
   * @param replicationManager - replication Manager.
   */
  public void setReplicationManager(ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  /**
   * Allows user to specify a custom version of Safe Mode Manager to use
   * with this SCM.
   * @param scmSafeModeManager - SafeMode Manager.
   */
  public void setScmSafeModeManager(SCMSafeModeManager scmSafeModeManager) {
    this.scmSafeModeManager = scmSafeModeManager;
  }

  /**
   * Allows user to specify a custom version of Certificate Server to use
   * with this SCM.
   * @param certificateAuthority - Certificate server.
   */
  public void setCertificateServer(CertificateServer certificateAuthority) {
    this.certificateServer = certificateAuthority;
  }

  /**
   * Allows user to specify a custom version of Metadata Store to  be used
   * with this SCM.
   * @param scmMetadataStore - scm metadata store.
   */
  public void setMetadataStore(SCMMetadataStore scmMetadataStore) {
    this.metadataStore = scmMetadataStore;
  }

  /**
   * Gets SCM Node Manager.
   * @return Node Manager.
   */
  public NodeManager getScmNodeManager() {
    return scmNodeManager;
  }

  /**
   * Get Pipeline Manager.
   * @return pipeline manager.
   */
  public PipelineManager getPipelineManager() {
    return pipelineManager;
  }

  /**
   * Get Container Manager.
   * @return container Manger.
   */
  public ContainerManager getContainerManager() {
    return containerManager;
  }

  /**
   * Get SCM Block Manager.
   * @return Block Manager.
   */
  public BlockManager getScmBlockManager() {
    return scmBlockManager;
  }

  /**
   * Get Replica Manager.
   * @return Replica Manager.
   */
  public ReplicationManager getReplicationManager() {
    return replicationManager;
  }

  /**
   * Gets Safe Mode Manager.
   * @return Safe Mode manager.
   */
  public SCMSafeModeManager getScmSafeModeManager() {
    return scmSafeModeManager;
  }

  /**
   * Get Certificate Manager.
   * @return Certificate Manager.
   */
  public CertificateServer getCertificateServer() {
    return certificateServer;
  }

  /**
   * Get Metadata Store.
   * @return SCMMetadataStore.
   */
  public SCMMetadataStore getMetadataStore() {
    return metadataStore;
  }
}
