package org.apache.hadoop.ozone.om.response.file;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;

/**
 * Response for crate file request.
 */
public class OMFileCreateResponse extends OMKeyCreateResponse {

  public OMFileCreateResponse(OmKeyInfo omKeyInfo, long openKeySessionID,
      OMResponse omResponse) {
    super(omKeyInfo, openKeySessionID, omResponse);
  }

}
