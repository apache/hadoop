package org.apache.hadoop.ozone.om.response.s3.multipart;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartKeyInfo;
import org.apache.hadoop.util.Time;

/**
 * Test multipart upload abort response.
 */
public class TestS3MultipartUploadAbortResponse
    extends TestS3MultipartResponse {


  @Test
  public void testAddDBToBatch() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String multipartUploadID = UUID.randomUUID().toString();
    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        createS3InitiateMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID);

    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    S3MultipartUploadAbortResponse s3MultipartUploadAbortResponse =
        createS3AbortMPUResponse(multipartKey, Time.now(),
            s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo());

    s3MultipartUploadAbortResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    // As no parts are created, so no entries should be there in delete table.
    Assert.assertTrue(omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()) == 0);
  }

  @Test
  public void testAddDBToBatchWithParts() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String multipartUploadID = UUID.randomUUID().toString();
    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        createS3InitiateMPUResponse(volumeName, bucketName, keyName,
            multipartUploadID);

    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
        batchOperation);


    // Add some dummy parts for testing.
    // Not added any key locations, as this just test is to see entries are
    // adding to delete table or not.

    OmMultipartKeyInfo omMultipartKeyInfo =
        s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo();

    PartKeyInfo part1 = createPartKeyInfo(volumeName, bucketName,
        keyName, 1);
    PartKeyInfo part2 = createPartKeyInfo(volumeName, bucketName,
        keyName, 2);

    addPart(1, part1, omMultipartKeyInfo);
    addPart(2, part2, omMultipartKeyInfo);


    long timeStamp = Time.now();
    S3MultipartUploadAbortResponse s3MultipartUploadAbortResponse =
        createS3AbortMPUResponse(multipartKey, timeStamp,
            s3InitiateMultipartUploadResponse.getOmMultipartKeyInfo());

    s3MultipartUploadAbortResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

    // As 2 parts are created, so 2 entries should be there in delete table.
    Assert.assertTrue(omMetadataManager.countRowsInTable(
        omMetadataManager.getDeletedTable()) == 2);

    String part1DeletedKeyName = OmUtils.getDeletedKeyName(
        omMultipartKeyInfo.getPartKeyInfo(1).getPartName(),
        timeStamp);

    String part2DeletedKeyName = OmUtils.getDeletedKeyName(
        omMultipartKeyInfo.getPartKeyInfo(2).getPartName(),
        timeStamp);

    Assert.assertNotNull(omMetadataManager.getDeletedTable().get(
        part1DeletedKeyName));
    Assert.assertNotNull(omMetadataManager.getDeletedTable().get(
        part2DeletedKeyName));

    Assert.assertEquals(OmKeyInfo.getFromProtobuf(part1.getPartKeyInfo()),
        omMetadataManager.getDeletedTable().get(part1DeletedKeyName));

    Assert.assertEquals(OmKeyInfo.getFromProtobuf(part2.getPartKeyInfo()),
        omMetadataManager.getDeletedTable().get(part2DeletedKeyName));
  }

}
