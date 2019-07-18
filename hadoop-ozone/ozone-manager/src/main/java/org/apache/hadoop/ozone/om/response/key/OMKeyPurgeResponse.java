package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.request.key.OMKeyPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

import java.io.IOException;
import java.util.List;

/**
 * Response for {@link OMKeyPurgeRequest} request.
 */
public class OMKeyPurgeResponse extends OMClientResponse {

  private List<String> purgeKeyList;

  public OMKeyPurgeResponse(List<String> keyList, OMResponse omResponse) {
    super(omResponse);
    this.purgeKeyList = keyList;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      for (String key : purgeKeyList) {
        omMetadataManager.getDeletedTable().deleteWithBatch(batchOperation,
            key);
      }
    }
  }
}
