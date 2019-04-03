package org.apache.hadoop.ozone.protocolPB;

import java.io.IOException;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;

/**
 * Handler to handle OM requests in OM HA.
 */
public interface OzoneManagerHARequestHandler extends RequestHandler {

  /**
   * Handle start Transaction Requests from OzoneManager StateMachine.
   * @param omRequest
   * @return OMRequest - New OM Request which will be applied during apply
   * Transaction
   * @throws IOException
   */
  OMRequest handleStartTransaction(OMRequest omRequest) throws IOException;

  /**
   * Handle Apply Transaction Requests from OzoneManager StateMachine.
   * @param omRequest
   * @return OMResponse
   */
  OMResponse handleApplyTransaction(OMRequest omRequest);

}
