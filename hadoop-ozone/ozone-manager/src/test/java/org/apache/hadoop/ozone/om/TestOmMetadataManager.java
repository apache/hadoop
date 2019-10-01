package org.apache.hadoop.ozone.om;
import com.google.common.base.Optional;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;

/**
 * Tests OzoneManager MetadataManager.
 */
public class TestOmMetadataManager {

  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration ozoneConfiguration;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  @Before
  public void setup() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OZONE_OM_DB_DIRS,
        folder.getRoot().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
  }
  @Test
  public void testListBuckets() throws Exception {

    String volumeName1 = "volumeA";
    String prefixBucketNameWithOzoneOwner = "ozoneBucket";
    String prefixBucketNameWithHadoopOwner = "hadoopBucket";

    TestOMRequestUtils.addVolumeToDB(volumeName1, omMetadataManager);
    for (int i=1; i<= 100; i++) {
      if (i % 2 == 0) {
        addBucketsToCache(volumeName1, prefixBucketNameWithOzoneOwner + i);
      } else {
        addBucketsToCache(volumeName1, prefixBucketNameWithHadoopOwner + i);
      }
    }

    String volumeName2 = "volumeB";
    TestOMRequestUtils.addVolumeToDB(volumeName2, omMetadataManager);
    for (int i=1; i<= 100; i++) {
      if (i % 2 == 0) {
        addBucketsToCache(volumeName2, prefixBucketNameWithOzoneOwner + i);
      } else {
        addBucketsToCache(volumeName2, prefixBucketNameWithHadoopOwner + i);
      }
    }

    // List all buckets which have prefix ozoneBucket
    List<OmBucketInfo> omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            null, prefixBucketNameWithOzoneOwner, 100);

    Assert.assertEquals(omBucketInfoList.size(),  50);

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      Assert.assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithOzoneOwner));
    }


    omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            prefixBucketNameWithOzoneOwner + 10, prefixBucketNameWithOzoneOwner,
            100);

    Assert.assertEquals(omBucketInfoList.size(),  49);

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      Assert.assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithOzoneOwner));
      Assert.assertFalse(omBucketInfo.getBucketName().equals(
          prefixBucketNameWithOzoneOwner + 10));
    }



    omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
        null, prefixBucketNameWithHadoopOwner, 100);

    Assert.assertEquals(omBucketInfoList.size(),  50);

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      Assert.assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithHadoopOwner));
    }

    String startBucket = null;
    for (int i=0; i<5; i++) {
      omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
          startBucket, prefixBucketNameWithHadoopOwner, 10);

      Assert.assertEquals(omBucketInfoList.size(), 10);

      for (OmBucketInfo omBucketInfo : omBucketInfoList) {
        Assert.assertTrue(omBucketInfo.getBucketName().startsWith(
            prefixBucketNameWithHadoopOwner));
        startBucket =  omBucketInfo.getBucketName();
      }
    }

    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
        startBucket, prefixBucketNameWithHadoopOwner, 10);

    Assert.assertEquals(omBucketInfoList.size(), 0);

  }


  private void addBucketsToCache(String volumeName, String bucketName) {

    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();

    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        new CacheValue<>(Optional.of(omBucketInfo), 1));
  }

}