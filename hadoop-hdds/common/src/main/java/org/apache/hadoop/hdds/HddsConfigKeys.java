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
package org.apache.hadoop.hdds;

import org.apache.hadoop.utils.db.DBProfile;

/**
 * This class contains constants for configuration keys and default values
 * used in hdds.
 */
public final class HddsConfigKeys {

  public static final String HDDS_HEARTBEAT_INTERVAL =
      "hdds.heartbeat.interval";
  public static final String HDDS_HEARTBEAT_INTERVAL_DEFAULT =
      "30s";
  public static final String HDDS_NODE_REPORT_INTERVAL =
      "hdds.node.report.interval";
  public static final String HDDS_NODE_REPORT_INTERVAL_DEFAULT =
      "60s";
  public static final String HDDS_CONTAINER_REPORT_INTERVAL =
      "hdds.container.report.interval";
  public static final String HDDS_CONTAINER_REPORT_INTERVAL_DEFAULT =
      "60s";
  public static final String HDDS_PIPELINE_REPORT_INTERVAL =
          "hdds.pipeline.report.interval";
  public static final String HDDS_PIPELINE_REPORT_INTERVAL_DEFAULT =
          "60s";
  public static final String HDDS_COMMAND_STATUS_REPORT_INTERVAL =
      "hdds.command.status.report.interval";
  public static final String HDDS_COMMAND_STATUS_REPORT_INTERVAL_DEFAULT =
      "60s";
  public static final String HDDS_CONTAINER_ACTION_MAX_LIMIT =
      "hdds.container.action.max.limit";
  public static final int HDDS_CONTAINER_ACTION_MAX_LIMIT_DEFAULT =
      20;
  public static final String HDDS_PIPELINE_ACTION_MAX_LIMIT =
      "hdds.pipeline.action.max.limit";
  public static final int HDDS_PIPELINE_ACTION_MAX_LIMIT_DEFAULT =
      20;
  // Configuration to allow volume choosing policy.
  public static final String HDDS_DATANODE_VOLUME_CHOOSING_POLICY =
      "hdds.datanode.volume.choosing.policy";
  // DB PKIProfile used by ROCKDB instances.
  public static final String HDDS_DB_PROFILE = "hdds.db.profile";
  public static final DBProfile HDDS_DEFAULT_DB_PROFILE = DBProfile.DISK;
  // Once a container usage crosses this threshold, it is eligible for
  // closing.
  public static final String HDDS_CONTAINER_CLOSE_THRESHOLD =
      "hdds.container.close.threshold";
  public static final float HDDS_CONTAINER_CLOSE_THRESHOLD_DEFAULT = 0.9f;
  public static final String HDDS_SCM_CHILLMODE_ENABLED =
      "hdds.scm.chillmode.enabled";
  public static final boolean HDDS_SCM_CHILLMODE_ENABLED_DEFAULT = true;
  public static final String HDDS_SCM_CHILLMODE_MIN_DATANODE =
      "hdds.scm.chillmode.min.datanode";
  public static final int HDDS_SCM_CHILLMODE_MIN_DATANODE_DEFAULT = 1;

  public static final String HDDS_SCM_CHILLMODE_PIPELINE_AVAILABILITY_CHECK =
      "hdds.scm.chillmode.pipeline-availability.check";
  public static final boolean
      HDDS_SCM_CHILLMODE_PIPELINE_AVAILABILITY_CHECK_DEFAULT = false;

  // % of containers which should have at least one reported replica
  // before SCM comes out of chill mode.
  public static final String HDDS_SCM_CHILLMODE_THRESHOLD_PCT =
      "hdds.scm.chillmode.threshold.pct";
  public static final double HDDS_SCM_CHILLMODE_THRESHOLD_PCT_DEFAULT = 0.99;
  public static final String HDDS_LOCK_MAX_CONCURRENCY =
      "hdds.lock.max.concurrency";
  public static final int HDDS_LOCK_MAX_CONCURRENCY_DEFAULT = 100;
  // This configuration setting is used as a fallback location by all
  // Ozone/HDDS services for their metadata. It is useful as a single
  // config point for test/PoC clusters.
  //
  // In any real cluster where performance matters, the SCM, OM and DN
  // metadata locations must be configured explicitly.
  public static final String OZONE_METADATA_DIRS = "ozone.metadata.dirs";

  public static final String HDDS_PROMETHEUS_ENABLED =
      "hdds.prometheus.endpoint.enabled";

  public static final String HDDS_PROFILER_ENABLED =
      "hdds.profiler.endpoint.enabled";

  public static final String HDDS_KEY_LEN = "hdds.key.len";
  public static final int HDDS_DEFAULT_KEY_LEN = 2048;
  public static final String HDDS_KEY_ALGORITHM = "hdds.key.algo";
  public static final String HDDS_DEFAULT_KEY_ALGORITHM = "RSA";
  public static final String HDDS_SECURITY_PROVIDER = "hdds.security.provider";
  public static final String HDDS_DEFAULT_SECURITY_PROVIDER = "BC";
  public static final String HDDS_KEY_DIR_NAME = "hdds.key.dir.name";
  public static final String HDDS_KEY_DIR_NAME_DEFAULT = "keys";
  // TODO : Talk to StorageIO classes and see if they can return a secure
  // storage location for each node.
  public static final String HDDS_METADATA_DIR_NAME = "hdds.metadata.dir";
  public static final String HDDS_PRIVATE_KEY_FILE_NAME =
      "hdds.priv.key.file.name";
  public static final String HDDS_PRIVATE_KEY_FILE_NAME_DEFAULT = "private.pem";
  public static final String HDDS_PUBLIC_KEY_FILE_NAME = "hdds.public.key.file"
      + ".name";
  public static final String HDDS_PUBLIC_KEY_FILE_NAME_DEFAULT = "public.pem";

  public static final String HDDS_BLOCK_TOKEN_EXPIRY_TIME =
      "hdds.block.token.expiry.time";
  public static final String HDDS_BLOCK_TOKEN_EXPIRY_TIME_DEFAULT = "1d";
  /**
   * Maximum duration of certificates issued by SCM including Self-Signed Roots.
   * The formats accepted are based on the ISO-8601 duration format PnDTnHnMn.nS
   * Default value is 5 years and written as P1865D.
   */
  public static final String HDDS_X509_MAX_DURATION = "hdds.x509.max.duration";
  // Limit Certificate duration to a max value of 5 years.
  public static final String HDDS_X509_MAX_DURATION_DEFAULT= "P1865D";
  public static final String HDDS_X509_SIGNATURE_ALGO =
      "hdds.x509.signature.algorithm";
  public static final String HDDS_X509_SIGNATURE_ALGO_DEFAULT = "SHA256withRSA";
  public static final String HDDS_BLOCK_TOKEN_ENABLED =
      "hdds.block.token.enabled";
  public static final boolean HDDS_BLOCK_TOKEN_ENABLED_DEFAULT = false;

  public static final String HDDS_X509_DIR_NAME = "hdds.x509.dir.name";
  public static final String HDDS_X509_DIR_NAME_DEFAULT = "certs";
  public static final String HDDS_X509_FILE_NAME = "hdds.x509.file.name";
  public static final String HDDS_X509_FILE_NAME_DEFAULT = "certificate.crt";

  /**
   * Default duration of certificates issued by SCM CA.
   * The formats accepted are based on the ISO-8601 duration format PnDTnHnMn.nS
   * Default value is 5 years and written as P1865D.
   */
  public static final String HDDS_X509_DEFAULT_DURATION = "hdds.x509.default" +
      ".duration";
  // Default Certificate duration to one year.
  public static final String HDDS_X509_DEFAULT_DURATION_DEFAULT = "P365D";

  /**
   * Do not instantiate.
   */
  private HddsConfigKeys() {
  }

  public static final String HDDS_GRPC_TLS_ENABLED = "hdds.grpc.tls.enabled";
  public static final boolean HDDS_GRPC_TLS_ENABLED_DEFAULT = false;

  public static final String HDDS_GRPC_MUTUAL_TLS_REQUIRED =
      "hdds.grpc.mutual.tls.required";
  public static final boolean HDDS_GRPC_MUTUAL_TLS_REQUIRED_DEFAULT = false;

  public static final String HDDS_GRPC_TLS_PROVIDER = "hdds.grpc.tls.provider";
  public static final String HDDS_GRPC_TLS_PROVIDER_DEFAULT = "OPENSSL";

  public static final String HDDS_TRUST_STORE_FILE_NAME =
      "hdds.trust.cert.collection.file.name";
  public static final String HDDS_TRUST_STORE_FILE_NAME_DEFAULT = "ca.crt";

  public static final String
      HDDS_SERVER_CERTIFICATE_CHAIN_FILE_NAME =
      "hdds.server.cert.chain.file.name";
  public static final String
      HDDS_SERVER_CERTIFICATE_CHAIN_FILE_NAME_DEFAULT = "server.crt";

  public static final String
      HDDS_CLIENT_CERTIFICATE_CHAIN_FILE_NAME =
      "hdds.client.cert.chain.file.name";
  public static final String
      HDDS_CLIENT_CERTIFICATE_CHAIN_FILE_NAME_DEFAULT = "client.crt";

  public static final String HDDS_GRPC_TLS_TEST_CERT = "hdds.grpc.tls" +
      ".test_cert";
  public static final boolean HDDS_GRPC_TLS_TEST_CERT_DEFAULT = false;
}