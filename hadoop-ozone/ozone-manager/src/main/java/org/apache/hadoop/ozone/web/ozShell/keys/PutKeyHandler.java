/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.ozShell.keys;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;

import org.apache.commons.codec.digest.DigestUtils;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Puts a file into an ozone bucket.
 */
@Command(name = "put",
    description = "creates or overwrites an existing key")
public class PutKeyHandler extends Handler {

  @Parameters(index = "0", arity = "1..1", description =
      Shell.OZONE_KEY_URI_DESCRIPTION)
  private String uri;

  @Parameters(index = "1", arity = "1..1", description = "File to upload")
  private String fileName;

  @Option(names = {"-r", "--replication"},
      description = "Replication factor of the new key. (use ONE or THREE) "
          + "Default is specified in the cluster-wide config.")
  private ReplicationFactor replicationFactor;
  /**
   * Executes the Client Calls.
   */
  @Override
  public Void call() throws Exception {

    OzoneAddress address = new OzoneAddress(uri);
    address.ensureKeyAddress();
    OzoneClient client = address.createClient(createOzoneConfiguration());

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    if (isVerbose()) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("Bucket Name : %s%n", bucketName);
      System.out.printf("Key Name : %s%n", keyName);
    }

    File dataFile = new File(fileName);

    if (isVerbose()) {
      FileInputStream stream = new FileInputStream(dataFile);
      String hash = DigestUtils.md5Hex(stream);
      System.out.printf("File Hash : %s%n", hash);
      stream.close();
    }

    Configuration conf = new OzoneConfiguration();
    if (replicationFactor == null) {
      replicationFactor = ReplicationFactor.valueOf(
          conf.getInt(OZONE_REPLICATION, OZONE_REPLICATION_DEFAULT));
    }

    ReplicationType replicationType = ReplicationType.valueOf(
        conf.get(OZONE_REPLICATION_TYPE, OZONE_REPLICATION_TYPE_DEFAULT));
    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    OzoneOutputStream outputStream = bucket
        .createKey(keyName, dataFile.length(), replicationType,
            replicationFactor, new HashMap<>());
    FileInputStream fileInputStream = new FileInputStream(dataFile);
    IOUtils.copyBytes(fileInputStream, outputStream, (int) conf
        .getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, OZONE_SCM_CHUNK_SIZE_DEFAULT,
            StorageUnit.BYTES));
    outputStream.close();
    fileInputStream.close();
    return null;
  }

}
