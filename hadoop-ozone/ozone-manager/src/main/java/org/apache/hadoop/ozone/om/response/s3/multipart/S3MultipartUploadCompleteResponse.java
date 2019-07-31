package org.apache.hadoop.ozone.om.response.s3.multipart;

import java.io.IOException;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

import javax.annotation.Nullable;

/**
 * Response for Multipart Upload Complete request.
 */
public class S3MultipartUploadCompleteResponse extends OMClientResponse {
  private String multipartKey;
  private OmKeyInfo omKeyInfo;


  public S3MultipartUploadCompleteResponse(@Nullable String multipartKey,
      @Nullable OmKeyInfo omKeyInfo, OMResponse omResponse) {
    super(omResponse);
    this.multipartKey = multipartKey;
    this.omKeyInfo = omKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      omMetadataManager.getKeyTable().putWithBatch(batchOperation,
          omMetadataManager.getOzoneKey(omKeyInfo.getVolumeName(),
              omKeyInfo.getBucketName(), omKeyInfo.getKeyName()), omKeyInfo);
      omMetadataManager.getOpenKeyTable().deleteWithBatch(batchOperation,
          multipartKey);
      omMetadataManager.getMultipartInfoTable().deleteWithBatch(batchOperation,
          multipartKey);
    }
  }
}


