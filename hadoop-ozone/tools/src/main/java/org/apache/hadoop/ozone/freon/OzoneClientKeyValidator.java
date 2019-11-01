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

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;

import com.codahale.metrics.Timer;
import org.apache.commons.io.IOUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator tool test om performance.
 */
@Command(name = "ockv",
    aliases = "ozone-client-key-validator",
    description = "Validate keys with the help of the ozone clients.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class OzoneClientKeyValidator extends BaseFreonGenerator
    implements Callable<Void> {

  @Option(names = {"-v", "--volume"},
      description = "Name of the bucket which contains the test data. Will be"
          + " created if missing.",
      defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"-b", "--bucket"},
      description = "Name of the bucket which contains the test data.",
      defaultValue = "bucket1")
  private String bucketName;

  private Timer timer;

  private byte[] referenceDigest;

  private OzoneClient rpcClient;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

    rpcClient = OzoneClientFactory.getRpcClient(ozoneConfiguration);

    try (InputStream stream = rpcClient.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName).readKey(generateObjectName(0))) {
      referenceDigest = getDigest(stream);
    }

    timer = getMetrics().timer("key-validate");

    runTests(this::validateKey);

    return null;
  }

  private void validateKey(long counter) throws Exception {
    String objectName = generateObjectName(counter);

    byte[] content = timer.time(() -> {
      try (InputStream stream = rpcClient.getObjectStore().getVolume(volumeName)
          .getBucket(bucketName).readKey(objectName)) {
        return IOUtils.toByteArray(stream);
      }
    });
    if (!MessageDigest.isEqual(referenceDigest, getDigest(content))) {
      throw new IllegalStateException(
          "Reference (=first) message digest doesn't match with digest of "
              + objectName);
    }
  }

}
