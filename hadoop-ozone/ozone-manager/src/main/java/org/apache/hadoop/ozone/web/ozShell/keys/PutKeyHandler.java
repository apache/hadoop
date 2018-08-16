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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.Shell;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT;

/**
 * Puts a file into an ozone bucket.
 */
public class PutKeyHandler extends Handler {
  private String volumeName;
  private String bucketName;
  private String keyName;

  /**
   * Executes the Client Calls.
   *
   * @param cmd - CommandLine
   * @throws IOException
   * @throws OzoneException
   * @throws URISyntaxException
   */
  @Override
  protected void execute(CommandLine cmd)
      throws IOException, OzoneException, URISyntaxException {
    if (!cmd.hasOption(Shell.PUT_KEY)) {
      throw new OzoneClientException("Incorrect call : putKey is missing");
    }

    if (!cmd.hasOption(Shell.FILE)) {
      throw new OzoneClientException("put key needs a file to put");
    }

    String ozoneURIString = cmd.getOptionValue(Shell.PUT_KEY);
    URI ozoneURI = verifyURI(ozoneURIString);
    Path path = Paths.get(ozoneURI.getPath());
    if (path.getNameCount() < 3) {
      throw new OzoneClientException(
          "volume/bucket/key name required in putKey");
    }

    volumeName = path.getName(0).toString();
    bucketName = path.getName(1).toString();
    keyName = path.getName(2).toString();


    if (cmd.hasOption(Shell.VERBOSE)) {
      System.out.printf("Volume Name : %s%n", volumeName);
      System.out.printf("Bucket Name : %s%n", bucketName);
      System.out.printf("Key Name : %s%n", keyName);
    }

    String fileName = cmd.getOptionValue(Shell.FILE);
    File dataFile = new File(fileName);

    if (cmd.hasOption(Shell.VERBOSE)) {
      FileInputStream stream = new FileInputStream(dataFile);
      String hash = DigestUtils.md5Hex(stream);
      System.out.printf("File Hash : %s%n", hash);
      stream.close();
    }

    Configuration conf = new OzoneConfiguration();
    ReplicationFactor replicationFactor;
    if (cmd.hasOption(Shell.REPLICATION_FACTOR)) {
      replicationFactor = ReplicationFactor.valueOf(Integer.parseInt(cmd
          .getOptionValue(Shell.REPLICATION_FACTOR)));
    } else {
      replicationFactor = ReplicationFactor.valueOf(
          conf.getInt(OZONE_REPLICATION, OZONE_REPLICATION_DEFAULT));
    }

    ReplicationType replicationType = ReplicationType.valueOf(
        conf.get(OZONE_REPLICATION_TYPE, OZONE_REPLICATION_TYPE_DEFAULT));
    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    OzoneOutputStream outputStream = bucket
        .createKey(keyName, dataFile.length(), replicationType,
            replicationFactor);
    FileInputStream fileInputStream = new FileInputStream(dataFile);
    IOUtils.copyBytes(fileInputStream, outputStream,
        conf.getInt(OZONE_SCM_CHUNK_SIZE_KEY, OZONE_SCM_CHUNK_SIZE_DEFAULT));
    outputStream.close();
    fileInputStream.close();
  }

}
