/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.File;
import java.io.IOException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.net.ServerSocketUtil;

import static org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBClientFactory.DefaultDynamoDBClientFactory.getRegion;

/**
 * A DynamoDBClientFactory implementation that creates AmazonDynamoDB clients
 * against an in-memory DynamoDBLocal server instance.
 *
 * You won't be charged bills for issuing any DynamoDB requests. However, the
 * DynamoDBLocal is considered a simulator of the DynamoDB web service, so it
 * may be stale or different. For example, the throttling is not yet supported
 * in DynamoDBLocal. This is for testing purpose only.
 *
 * To use this for creating DynamoDB client in tests:
 * <ol>
 * <li>
 *    As all DynamoDBClientFactory implementations, this should be configured.
 * </li>
 * <li>
 *    The singleton DynamoDBLocal server instance is started automatically when
 *    creating the AmazonDynamoDB client for the first time. It still merits to
 *    launch the server before all the tests and fail fast if error happens.
 * </li>
 * <li>
 *    The server can be stopped explicitly, which is not actually needed in
 *    tests as JVM termination will do that.
 * </li>
 * </ol>
 *
 * @see DefaultDynamoDBClientFactory
 */
public class DynamoDBLocalClientFactory extends Configured
    implements DynamoDBClientFactory {

  /** The DynamoDBLocal dynamoDBLocalServer instance for testing. */
  private static DynamoDBProxyServer dynamoDBLocalServer;
  private static String ddbEndpoint;

  private static final String SYSPROP_SQLITE_LIB = "sqlite4java.library.path";

  @Override
  public AmazonDynamoDB createDynamoDBClient(String defaultRegion)
      throws IOException {
    startSingletonServer();

    final Configuration conf = getConf();
    final AWSCredentialsProvider credentials =
        createAWSCredentialProviderSet(null, conf);
    final ClientConfiguration awsConf =
        DefaultS3ClientFactory.createAwsConf(conf);
    // fail fast in case of service errors
    awsConf.setMaxErrorRetry(3);

    final String region = getRegion(conf, defaultRegion);
    LOG.info("Creating DynamoDBLocal client using endpoint {} in region {}",
        ddbEndpoint, region);

    return AmazonDynamoDBClientBuilder.standard()
        .withCredentials(credentials)
        .withClientConfiguration(awsConf)
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(ddbEndpoint, region))
        .build();
  }

  /**
   * Start a singleton in-memory DynamoDBLocal server if not started yet.
   * @throws IOException if any error occurs
   */
  public synchronized static void startSingletonServer() throws IOException {
    if (dynamoDBLocalServer != null) {
      return;
    }

    // Set this property if it has not been set elsewhere
    if (StringUtils.isEmpty(System.getProperty(SYSPROP_SQLITE_LIB))) {
      String projectBuildDir = System.getProperty("project.build.directory");
      if (StringUtils.isEmpty(projectBuildDir)) {
        projectBuildDir = "target";
      }
      // sqlite4java lib should have been copied to $projectBuildDir/native-libs
      System.setProperty(SYSPROP_SQLITE_LIB,
          projectBuildDir + File.separator + "native-libs");
      LOG.info("Setting {} -> {}",
          SYSPROP_SQLITE_LIB, System.getProperty(SYSPROP_SQLITE_LIB));
    }

    try {
      // Start an in-memory local DynamoDB instance
      final String port = String.valueOf(ServerSocketUtil.getPort(0, 100));
      ddbEndpoint = "http://localhost:" + port;
      dynamoDBLocalServer = ServerRunner.createServerFromCommandLineArgs(
          new String[]{"-inMemory", "-port", port});
      dynamoDBLocalServer.start();
      LOG.info("DynamoDBLocal singleton server was started at {}", ddbEndpoint);
    } catch (Exception t) {
      String msg = "Error starting DynamoDBLocal server at " + ddbEndpoint
          + " " + t;
      LOG.error(msg, t);
      throw new IOException(msg, t);
    }
  }

  /**
   * Stop the in-memory DynamoDBLocal server if it is started.
   * @throws IOException if any error occurs
   */
  public synchronized static void stopSingletonServer() throws IOException {
    if (dynamoDBLocalServer != null) {
      LOG.info("Shutting down the in-memory DynamoDBLocal server");
      try {
        dynamoDBLocalServer.stop();
      } catch (Throwable t) {
        String msg = "Error stopping DynamoDBLocal server at " + ddbEndpoint;
        LOG.error(msg, t);
        throw new IOException(msg, t);
      }
    }
  }

}
