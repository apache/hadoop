package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;
import org.apache.hadoop.util.Time;


/**
 * Tests S3 Multipart Upload Complete request.
 */
public class TestS3MultipartUploadCompleteRequest
    extends TestS3MultipartRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    doPreExecuteCompleteMPU(volumeName, bucketName, keyName,
        UUID.randomUUID().toString(), new ArrayList<>());
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        new S3InitiateMultipartUploadRequest(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            1L, ozoneManagerDoubleBufferHelper);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        new S3MultipartUploadCommitPartRequest(commitMultipartRequest);

    // Add key to open key table.
    TestOMRequestUtils.addKeyToTable(true, volumeName, bucketName,
        keyName, clientID, HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);

    s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
        2L, ozoneManagerDoubleBufferHelper);

    List<Part> partList = new ArrayList<>();

    partList.add(Part.newBuilder().setPartName(
        omMetadataManager.getOzoneKey(volumeName, bucketName, keyName) +
            clientID).setPartNumber(1).build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        new S3MultipartUploadCompleteRequest(completeMultipartRequest);

    omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager,
            3L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    Assert.assertNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));
    Assert.assertNotNull(omMetadataManager.getKeyTable().get(
        omMetadataManager.getOzoneKey(volumeName, bucketName, keyName)));
  }

  @Test
  public void testValidateAndUpdateCacheVolumeNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        new S3MultipartUploadCompleteRequest(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager,
            3L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeToDB(volumeName, omMetadataManager);
    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        new S3MultipartUploadCompleteRequest(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager,
            3L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheNoSuchMultipartUploadError()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    // Doing  complete multipart upload request with out initiate.
    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        new S3MultipartUploadCompleteRequest(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager,
            3L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omClientResponse.getOMResponse().getStatus());

  }
}

