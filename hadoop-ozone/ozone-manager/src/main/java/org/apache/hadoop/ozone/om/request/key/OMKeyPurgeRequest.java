package org.apache.hadoop.ozone.om.request.key;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Handles purging of keys from OM DB.
 */
public class OMKeyPurgeRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyPurgeRequest.class);

  public OMKeyPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    PurgeKeysRequest purgeKeysRequest = getOmRequest().getPurgeKeysRequest();
    List<String> purgeKeysList = purgeKeysRequest.getKeysList();

    LOG.debug("Processing Purge Keys for {} number of keys.",
        purgeKeysList.size());

    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(Type.PurgeKeys)
        .setPurgeKeysResponse(
            OzoneManagerProtocolProtos.PurgeKeysResponse.newBuilder().build())
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();

    OMClientResponse omClientResponse = new OMKeyPurgeResponse(purgeKeysList,
        omResponse);
    omClientResponse.setFlushFuture(
        ozoneManagerDoubleBufferHelper.add(omClientResponse,
            transactionLogIndex));
    return omClientResponse;
  }
}
