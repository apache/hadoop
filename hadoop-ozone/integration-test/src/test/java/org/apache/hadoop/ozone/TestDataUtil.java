package org.apache.hadoop.ozone;

import java.io.IOException;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;

import org.apache.commons.lang3.RandomStringUtils;

/**
 * Utility to help to generate test data.
 */
public final class TestDataUtil {

  private TestDataUtil() {
  }

  public static OzoneBucket createVolumeAndBucket(MiniOzoneCluster cluster,
      String volumeName, String bucketName) throws IOException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);

    OzoneClient client = cluster.getClient();

    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .setAdmin(adminName)
        .setOwner(userName)
        .build();

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName, volumeArgs);

    OzoneVolume volume = objectStore.getVolume(volumeName);

    BucketArgs omBucketArgs = BucketArgs.newBuilder()
        .setStorageType(StorageType.DISK)
        .build();

    volume.createBucket(bucketName, omBucketArgs);
    return volume.getBucket(bucketName);

  }

  public static void createKey(OzoneBucket bucket, String keyName,
      String content) {

  }

  public static String getKey(OzoneBucket bucket, String keyName) {
    return null;
  }

  public static OzoneBucket createVolumeAndBucket(MiniOzoneCluster cluster)
      throws IOException {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    return createVolumeAndBucket(cluster, volumeName, bucketName);
  }
}
