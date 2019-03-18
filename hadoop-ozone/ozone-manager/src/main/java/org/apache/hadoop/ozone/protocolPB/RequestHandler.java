package org.apache.hadoop.ozone.protocolPB;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.
    OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.
    OMResponse;

/**
 * Handler to handle the OmRequests.
 */
public interface RequestHandler {


  /**
   * Handle the OmRequest, and returns OmResponse.
   * @param request
   * @return OmResponse
   */
  OMResponse handle(OMRequest request);

  /**
   * Validates that the incoming OM request has required parameters.
   * TODO: Add more validation checks before writing the request to Ratis log.
   *
   * @param omRequest client request to OM
   * @throws OMException thrown if required parameters are set to null.
   */
  void validateRequest(OMRequest omRequest) throws OMException;

}
