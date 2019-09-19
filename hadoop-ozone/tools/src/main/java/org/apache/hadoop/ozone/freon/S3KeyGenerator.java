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

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Generate random keys via the s3 interface.
 */
@Command(name = "s3kg",
    aliases = "s3-key-generator",
    description = "Create random keys via the s3 interface.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class S3KeyGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3KeyGenerator.class);

  @Option(names = {"-b", "--bucket"},
      description =
          "Name of the (S3!) bucket which contains the test data.",
      defaultValue = "bucket1")
  private String bucketName;

  @Option(names = {"-s", "--size"},
      description = "Size of the generated key (in bytes)",
      defaultValue = "10240")
  private int fileSize;

  @Option(names = {"-e", "--endpoint"},
      description = "S3 HTTP endpoint",
      defaultValue = "http://localhost:9878")
  private String endpoint;

  private Timer timer;

  private String content;

  private AmazonS3 s3;

  @Override
  public Void call() throws Exception {

    init();

    AmazonS3ClientBuilder amazonS3ClientBuilder =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new EnvironmentVariableCredentialsProvider());

    if (endpoint.length() > 0) {
      amazonS3ClientBuilder
          .withPathStyleAccessEnabled(true)
          .withEndpointConfiguration(new EndpointConfiguration(endpoint, ""));

    } else {
      amazonS3ClientBuilder.withRegion(Regions.DEFAULT_REGION);
    }

    s3 = amazonS3ClientBuilder.build();

    content = RandomStringUtils.randomAscii(fileSize);

    timer = getMetrics().timer("key-create");

    runTests(this::createKey);

    return null;
  }

  private void createKey(long counter) throws Exception {
    timer.time(() -> {

      s3.putObject(bucketName, generateObjectName(counter),
          content);
      return null;
    });
  }
}
