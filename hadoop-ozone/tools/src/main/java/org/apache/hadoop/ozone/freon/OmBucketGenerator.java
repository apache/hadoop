/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import com.codahale.metrics.Timer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "ombg",
    aliases = "om-bucket-generator",
    description = "Generate ozone buckets on OM side.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class OmBucketGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  private OzoneManagerProtocol ozoneManagerClient;

  private Timer bucketCreationTimer;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    ozoneManagerClient = createOmClient(ozoneConfiguration);

    ensureVolumeExists(ozoneConfiguration, volumeName);

    bucketCreationTimer = getMetrics().timer("bucket-create");

    runTests(this::createBucket);

    return null;
  }

  private void createBucket(long index) throws Exception {

    OmBucketInfo bucketInfo = new OmBucketInfo.Builder()
        .setBucketName(getPrefix()+index)
        .setVolumeName(volumeName)
        .setStorageType(StorageType.DISK)
        .build();

    bucketCreationTimer.time(() -> {
      ozoneManagerClient.createBucket(bucketInfo);
      return null;
    });
  }

}
