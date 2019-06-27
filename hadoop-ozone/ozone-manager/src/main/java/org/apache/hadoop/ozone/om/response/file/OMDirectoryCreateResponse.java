package org.apache.hadoop.ozone.om.response.file;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Response for create directory request.
 */
public class OMDirectoryCreateResponse extends OMClientResponse {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateResponse.class);
  private OmKeyInfo dirKeyInfo;

  public OMDirectoryCreateResponse(OmKeyInfo dirKeyInfo,
      OMResponse omResponse) {
    super(omResponse);
    this.dirKeyInfo = dirKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      if (dirKeyInfo != null) {
        String dirKey =
            omMetadataManager.getOzoneKey(dirKeyInfo.getVolumeName(),
                dirKeyInfo.getBucketName(), dirKeyInfo.getKeyName());
        omMetadataManager.getKeyTable().putWithBatch(batchOperation, dirKey,
            dirKeyInfo);
      } else {
        // When directory already exists, we don't add it to cache. And it is
        // not an error, in this case dirKeyInfo will be null.
        LOG.debug("Response Status is OK, dirKeyInfo is null in " +
            "OMDirectoryCreateResponse");
      }
    }
  }
}
