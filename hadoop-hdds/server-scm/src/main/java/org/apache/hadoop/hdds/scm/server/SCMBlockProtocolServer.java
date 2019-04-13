/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import com.google.common.collect.Maps;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.DeleteBlockResult;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.SCMAction;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.protocolPB
    .ScmBlockLocationProtocolServerSideTranslatorPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdds.server.ServerUtils.updateRPCListenAddress;
import static org.apache.hadoop.hdds.scm.server.StorageContainerManager
    .startRpcServer;

/**
 * SCM block protocol is the protocol used by Namenode and OzoneManager to get
 * blocks from the SCM.
 */
public class SCMBlockProtocolServer implements
    ScmBlockLocationProtocol, Auditor {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMBlockProtocolServer.class);

  private static final AuditLogger AUDIT =
      new AuditLogger(AuditLoggerType.SCMLOGGER);

  private final StorageContainerManager scm;
  private final OzoneConfiguration conf;
  private final RPC.Server blockRpcServer;
  private final InetSocketAddress blockRpcAddress;

  /**
   * The RPC server that listens to requests from block service clients.
   */
  public SCMBlockProtocolServer(OzoneConfiguration conf,
      StorageContainerManager scm) throws IOException {
    this.scm = scm;
    this.conf = conf;
    final int handlerCount =
        conf.getInt(OZONE_SCM_HANDLER_COUNT_KEY,
            OZONE_SCM_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(conf, ScmBlockLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    // SCM Block Service RPC.
    BlockingService blockProtoPbService =
        ScmBlockLocationProtocolProtos.ScmBlockLocationProtocolService
            .newReflectiveBlockingService(
                new ScmBlockLocationProtocolServerSideTranslatorPB(this));

    final InetSocketAddress scmBlockAddress = HddsServerUtil
        .getScmBlockClientBindAddress(conf);
    blockRpcServer =
        startRpcServer(
            conf,
            scmBlockAddress,
            ScmBlockLocationProtocolPB.class,
            blockProtoPbService,
            handlerCount);
    blockRpcAddress =
        updateRPCListenAddress(
            conf, OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, scmBlockAddress,
            blockRpcServer);
    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      blockRpcServer.refreshServiceAcl(conf, SCMPolicyProvider.getInstance());
    }
  }

  public RPC.Server getBlockRpcServer() {
    return blockRpcServer;
  }

  public InetSocketAddress getBlockRpcAddress() {
    return blockRpcAddress;
  }

  public void start() {
    LOG.info(
        StorageContainerManager.buildRpcServerStartMessage(
            "RPC server for Block Protocol", getBlockRpcAddress()));
    getBlockRpcServer().start();
  }

  public void stop() {
    try {
      LOG.info("Stopping the RPC server for Block Protocol");
      getBlockRpcServer().stop();
    } catch (Exception ex) {
      LOG.error("Block Protocol RPC stop failed.", ex);
    }
    IOUtils.cleanupWithLogger(LOG, scm.getScmNodeManager());
  }

  public void join() throws InterruptedException {
    LOG.trace("Join RPC server for Block Protocol");
    getBlockRpcServer().join();
  }

  @Override
  public List<AllocatedBlock> allocateBlock(long size, int num,
      HddsProtos.ReplicationType type, HddsProtos.ReplicationFactor factor,
      String owner, ExcludeList excludeList) throws IOException {
    Map<String, String> auditMap = Maps.newHashMap();
    auditMap.put("size", String.valueOf(size));
    auditMap.put("type", type.name());
    auditMap.put("factor", factor.name());
    auditMap.put("owner", owner);
    List<AllocatedBlock> blocks = new ArrayList<>(num);
    boolean auditSuccess = true;
    try {
      for (int i = 0; i < num; i++) {
        AllocatedBlock block = scm.getScmBlockManager()
            .allocateBlock(size, type, factor, owner, excludeList);
        if (block != null) {
          blocks.add(block);
        }
      }
      return blocks;
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(SCMAction.ALLOCATE_BLOCK, auditMap, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.ALLOCATE_BLOCK, auditMap)
        );
      }
    }
  }

  /**
   * Delete blocks for a set of object keys.
   *
   * @param keyBlocksInfoList list of block keys with object keys to delete.
   * @return deletion results.
   */
  @Override
  public List<DeleteBlockGroupResult> deleteKeyBlocks(
      List<BlockGroup> keyBlocksInfoList) throws IOException {
    LOG.info("SCM is informed by OM to delete {} blocks", keyBlocksInfoList
        .size());
    List<DeleteBlockGroupResult> results = new ArrayList<>();
    Map<String, String> auditMap = Maps.newHashMap();
    for (BlockGroup keyBlocks : keyBlocksInfoList) {
      ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result resultCode;
      try {
        // We delete blocks in an atomic operation to prevent getting
        // into state like only a partial of blocks are deleted,
        // which will leave key in an inconsistent state.
        auditMap.put("keyBlockToDelete", keyBlocks.toString());
        scm.getScmBlockManager().deleteBlocks(keyBlocks.getBlockIDList());
        resultCode = ScmBlockLocationProtocolProtos.DeleteScmBlockResult
            .Result.success;
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(SCMAction.DELETE_KEY_BLOCK, auditMap)
        );
      } catch (SCMException scmEx) {
        LOG.warn("Fail to delete block: {}", keyBlocks.getGroupID(), scmEx);
        AUDIT.logWriteFailure(
            buildAuditMessageForFailure(SCMAction.DELETE_KEY_BLOCK, auditMap,
                scmEx)
        );
        switch (scmEx.getResult()) {
        case SAFE_MODE_EXCEPTION:
          resultCode = ScmBlockLocationProtocolProtos.DeleteScmBlockResult
              .Result.safeMode;
          break;
        case FAILED_TO_FIND_BLOCK:
          resultCode = ScmBlockLocationProtocolProtos.DeleteScmBlockResult
              .Result.errorNotFound;
          break;
        default:
          resultCode = ScmBlockLocationProtocolProtos.DeleteScmBlockResult
              .Result.unknownFailure;
        }
      } catch (IOException ex) {
        LOG.warn("Fail to delete blocks for object key: {}", keyBlocks
            .getGroupID(), ex);
        AUDIT.logWriteFailure(
            buildAuditMessageForFailure(SCMAction.DELETE_KEY_BLOCK, auditMap,
                ex)
        );
        resultCode = ScmBlockLocationProtocolProtos.DeleteScmBlockResult
            .Result.unknownFailure;
      }
      List<DeleteBlockResult> blockResultList = new ArrayList<>();
      for (BlockID blockKey : keyBlocks.getBlockIDList()) {
        blockResultList.add(new DeleteBlockResult(blockKey, resultCode));
      }
      results.add(new DeleteBlockGroupResult(keyBlocks.getGroupID(),
          blockResultList));
    }
    return results;
  }

  @Override
  public ScmInfo getScmInfo() throws IOException {
    boolean auditSuccess = true;
    try{
      ScmInfo.Builder builder =
          new ScmInfo.Builder()
              .setClusterId(scm.getScmStorageConfig().getClusterID())
              .setScmId(scm.getScmStorageConfig().getScmId());
      return builder.build();
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(SCMAction.GET_SCM_INFO, null, ex)
      );
      throw ex;
    } finally {
      if(auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(SCMAction.GET_SCM_INFO, null)
        );
      }
    }
  }

  @Override
  public AuditMessage buildAuditMessageForSuccess(
      AuditAction op, Map<String, String> auditMap) {
    return new AuditMessage.Builder()
        .setUser((Server.getRemoteUser() == null) ? null :
            Server.getRemoteUser().getUserName())
        .atIp((Server.getRemoteIp() == null) ? null :
            Server.getRemoteIp().getHostAddress())
        .forOperation(op.getAction())
        .withParams(auditMap)
        .withResult(AuditEventStatus.SUCCESS.toString())
        .withException(null)
        .build();
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op, Map<String,
      String> auditMap, Throwable throwable) {
    return new AuditMessage.Builder()
        .setUser((Server.getRemoteUser() == null) ? null :
            Server.getRemoteUser().getUserName())
        .atIp((Server.getRemoteIp() == null) ? null :
            Server.getRemoteIp().getHostAddress())
        .forOperation(op.getAction())
        .withParams(auditMap)
        .withResult(AuditEventStatus.FAILURE.toString())
        .withException(throwable)
        .build();
  }

  @Override
  public void close() throws IOException {
    stop();
  }
}
