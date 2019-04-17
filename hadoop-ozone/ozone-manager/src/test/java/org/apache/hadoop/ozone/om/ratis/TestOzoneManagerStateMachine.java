/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMNodeDetails;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerHARequestHandler;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerHARequestHandlerImpl;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * This class tests OzoneManagerStateMachine.
 */
public class TestOzoneManagerStateMachine {

  private OzoneConfiguration conf;
  private OzoneManagerRatisServer omRatisServer;
  private String omID;
  private OzoneManagerHARequestHandler requestHandler;
  private RaftGroupId raftGroupId;
  private OzoneManagerStateMachine ozoneManagerStateMachine;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();


  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    omID = UUID.randomUUID().toString();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().toString());
    int ratisPort = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT);
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    OMNodeDetails omNodeDetails = new OMNodeDetails.Builder()
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setOMNodeId(omID)
        .setOMServiceId(OzoneConsts.OM_SERVICE_ID_DEFAULT)
        .build();
    // Starts a single node Ratis server
    omRatisServer = OzoneManagerRatisServer.newOMRatisServer(conf, null,
        omNodeDetails, Collections.emptyList());


    ozoneManagerStateMachine =
        new OzoneManagerStateMachine(omRatisServer);

    requestHandler = Mockito.mock(OzoneManagerHARequestHandlerImpl.class);
    raftGroupId = omRatisServer.getRaftGroup().getGroupId();

    ozoneManagerStateMachine.setHandler(requestHandler);
    ozoneManagerStateMachine.setRaftGroupId(raftGroupId);

  }

  @Test
  public void testAllocateBlockRequestWithSuccess() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    long allocateBlockClientId = RandomUtils.nextLong();
    String clientId = UUID.randomUUID().toString();


    OMRequest omRequest = createOmRequestForAllocateBlock(volumeName,
        bucketName, keyName, allocateBlockClientId, clientId);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        createOmResponseForAllocateBlock(true);

    when(requestHandler.handle(omRequest)).thenReturn(omResponse);


    RaftClientRequest raftClientRequest =
        new RaftClientRequest(ClientId.randomId(),
            RaftPeerId.valueOf("random"), raftGroupId, 1,
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)),
            RaftClientRequest.Type.valueOf(
                RaftProtos.WriteRequestTypeProto.getDefaultInstance()), null);

    TransactionContext transactionContext =
        ozoneManagerStateMachine.startTransaction(raftClientRequest);


    OMRequest newOmRequest = OMRatisHelper.convertByteStringToOMRequest(
        transactionContext.getStateMachineLogEntry().getLogData());

    Assert.assertTrue(newOmRequest.hasAllocateBlockRequest());
    checkModifiedOmRequest(omRequest, newOmRequest);

    // Check this keyLocation, and the keyLocation is same as from OmResponse.
    Assert.assertTrue(newOmRequest.getAllocateBlockRequest().hasKeyLocation());

    Assert.assertEquals(omResponse.getAllocateBlockResponse().getKeyLocation(),
        newOmRequest.getAllocateBlockRequest().getKeyLocation());

  }


  private OMRequest createOmRequestForAllocateBlock(String volumeName,
      String bucketName, String keyName, long allocateClientId,
      String clientId) {
    //Create AllocateBlockRequest
    AllocateBlockRequest.Builder req = AllocateBlockRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(100).build();
    req.setKeyArgs(keyArgs);
    req.setClientID(allocateClientId);
    req.setExcludeList(HddsProtos.ExcludeListProto.getDefaultInstance());
    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .setAllocateBlockRequest(req)
        .setClientId(clientId)
        .build();
  }

  private OMResponse createOmResponseForAllocateBlock(boolean status) {
    OmKeyLocationInfo newLocation = new OmKeyLocationInfo.Builder().setBlockID(
        new BlockID(RandomUtils.nextLong(),
            RandomUtils.nextLong()))
        .setLength(RandomUtils.nextLong())
        .setOffset(0).setPipeline(
            Pipeline.newBuilder().setId(PipelineID.randomId())
                .setType(HddsProtos.ReplicationType.RATIS)
                .setFactor(HddsProtos.ReplicationFactor.THREE)
                .setState(Pipeline.PipelineState.OPEN)
                .setNodes(new ArrayList<>()).build()).build();

    OzoneManagerProtocolProtos.AllocateBlockResponse.Builder resp =
        OzoneManagerProtocolProtos.AllocateBlockResponse.newBuilder();
    resp.setKeyLocation(newLocation.getProtobuf());


    if (status) {
      return OzoneManagerProtocolProtos.OMResponse.newBuilder().setSuccess(true)
          .setAllocateBlockResponse(resp)
          .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
          .setStatus(OzoneManagerProtocolProtos.Status.OK)
          .setSuccess(status).build();
    } else {
      return OzoneManagerProtocolProtos.OMResponse.newBuilder().setSuccess(true)
          .setAllocateBlockResponse(resp)
          .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
          .setStatus(OzoneManagerProtocolProtos.Status.SCM_IN_SAFE_MODE)
          .setMessage("Scm in Safe mode")
          .setSuccess(status).build();
    }

  }
  @Test
  public void testAllocateBlockWithFailure() throws Exception{
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    long allocateBlockClientId = RandomUtils.nextLong();
    String clientId = UUID.randomUUID().toString();


    OMRequest omRequest = createOmRequestForAllocateBlock(volumeName,
        bucketName, keyName, allocateBlockClientId, clientId);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        createOmResponseForAllocateBlock(false);

    when(requestHandler.handle(omRequest)).thenReturn(omResponse);


    RaftClientRequest raftClientRequest =
        new RaftClientRequest(ClientId.randomId(),
            RaftPeerId.valueOf("random"), raftGroupId, 1,
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)),
            RaftClientRequest.Type.valueOf(
                RaftProtos.WriteRequestTypeProto.getDefaultInstance()), null);

    TransactionContext transactionContext =
        ozoneManagerStateMachine.startTransaction(raftClientRequest);


    OMRequest newOmRequest = OMRatisHelper.convertByteStringToOMRequest(
        transactionContext.getStateMachineLogEntry().getLogData());

    Assert.assertTrue(newOmRequest.hasAllocateBlockRequest());
    checkModifiedOmRequest(omRequest, newOmRequest);

    // As the request failed, check for keyLocation and  the transaction
    // context error message
    Assert.assertFalse(newOmRequest.getAllocateBlockRequest().hasKeyLocation());
    Assert.assertEquals("Scm in Safe mode " + OMException.STATUS_CODE
            + OMException.ResultCodes.SCM_IN_SAFE_MODE,
        transactionContext.getException().getMessage());
    Assert.assertTrue(transactionContext.getException() instanceof IOException);

  }

  private void checkModifiedOmRequest(OMRequest omRequest,
      OMRequest newOmRequest) {
    Assert.assertTrue(newOmRequest.getAllocateBlockRequest()
        .getKeyArgs().getBucketName().equals(
            omRequest.getAllocateBlockRequest().getKeyArgs().getBucketName()));
    Assert.assertTrue(omRequest.getAllocateBlockRequest()
        .getKeyArgs().getVolumeName().equals(
            newOmRequest.getAllocateBlockRequest().getKeyArgs()
                .getVolumeName()));
    Assert.assertTrue(omRequest.getAllocateBlockRequest()
        .getKeyArgs().getKeyName().equals(
            newOmRequest.getAllocateBlockRequest().getKeyArgs().getKeyName()));
    Assert.assertEquals(omRequest.getAllocateBlockRequest()
            .getKeyArgs().getDataSize(),
        newOmRequest.getAllocateBlockRequest().getKeyArgs().getDataSize());
    Assert.assertEquals(omRequest.getAllocateBlockRequest()
            .getClientID(),
        newOmRequest.getAllocateBlockRequest().getClientID());
    Assert.assertEquals(omRequest.getClientId(), newOmRequest.getClientId());
  }
}
