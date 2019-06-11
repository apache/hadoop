package org.apache.hadoop.ozone.om.response.key;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

import java.io.IOException;

/**
 * Response for RenameKey request.
 */
public class OMKeyRenameResponse extends OMClientResponse {

  private final OmKeyInfo renameKeyInfo;
  private final String toKeyName;
  private final String fromKeyName;

  public OMKeyRenameResponse(OmKeyInfo renameKeyInfo, String toKeyName,
      String fromKeyName, OMResponse omResponse) {
    super(omResponse);
    this.renameKeyInfo = renameKeyInfo;
    this.toKeyName = toKeyName;
    this.fromKeyName = fromKeyName;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {

      // If both from and toKeyName are equal do nothing
      if (!toKeyName.equals(fromKeyName)) {
        String volumeName = renameKeyInfo.getVolumeName();
        String bucketName = renameKeyInfo.getBucketName();
        omMetadataManager.getKeyTable().deleteWithBatch(batchOperation,
            omMetadataManager.getOzoneKey(volumeName, bucketName, fromKeyName));
        omMetadataManager.getKeyTable().putWithBatch(batchOperation,
            omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName),
            renameKeyInfo);
      }
    }
  }
}
