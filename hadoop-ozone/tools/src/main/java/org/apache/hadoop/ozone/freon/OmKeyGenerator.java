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

import java.util.ArrayList;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs.Builder;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;

import com.codahale.metrics.Timer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "omkg",
    aliases = "om-key-generator",
    description = "Create keys to the om metadata table.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class OmKeyGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "bucket1")
  private String bucketName;

  private OzoneManagerProtocol ozoneManagerClient;

  private Timer timer;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    ensureVolumeAndBucketExist(ozoneConfiguration, volumeName, bucketName);

    ozoneManagerClient = createOmClient(ozoneConfiguration);

    timer = getMetrics().timer("key-create");

    runTests(this::createKey);

    return null;
  }

  private void createKey(long counter) throws Exception {

    OmKeyArgs keyArgs = new Builder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setType(ReplicationType.RATIS)
        .setFactor(ReplicationFactor.THREE)
        .setKeyName(generateObjectName(counter))
        .setLocationInfoList(new ArrayList<>())
        .build();

    timer.time(() -> {
      OpenKeySession openKeySession = ozoneManagerClient.openKey(keyArgs);

      ozoneManagerClient.commitKey(keyArgs, openKeySession.getId());
      return null;
    });
  }

}
