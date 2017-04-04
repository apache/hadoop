/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.DeprecationDelta;

/**
 * Constants.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class AdlConfKeys {
  // OAuth2 Common Configuration
  public static final String AZURE_AD_REFRESH_URL_KEY =
      "fs.adl.oauth2.refresh.url";

  // optional when provider type is refresh or client id.
  public static final String AZURE_AD_TOKEN_PROVIDER_CLASS_KEY =
      "fs.adl.oauth2.access.token.provider";
  public static final String AZURE_AD_CLIENT_ID_KEY =
      "fs.adl.oauth2.client.id";
  public static final String AZURE_AD_TOKEN_PROVIDER_TYPE_KEY =
      "fs.adl.oauth2.access.token.provider.type";

  // OAuth Refresh Token Configuration
  public static final String AZURE_AD_REFRESH_TOKEN_KEY =
      "fs.adl.oauth2.refresh.token";

  public static final String TOKEN_PROVIDER_TYPE_REFRESH_TOKEN = "RefreshToken";
  // OAuth Client Cred Token Configuration
  public static final String AZURE_AD_CLIENT_SECRET_KEY =
      "fs.adl.oauth2.credential";
  public static final String TOKEN_PROVIDER_TYPE_CLIENT_CRED =
      "ClientCredential";

  public static final String READ_AHEAD_BUFFER_SIZE_KEY =
      "adl.feature.client.cache.readahead";

  public static final String WRITE_BUFFER_SIZE_KEY =
      "adl.feature.client.cache.drop.behind.writes";
  static final String SECURE_TRANSPORT_SCHEME = "https";
  static final String INSECURE_TRANSPORT_SCHEME = "http";
  static final String ADL_DEBUG_OVERRIDE_LOCAL_USER_AS_OWNER =
      "adl.debug.override.localuserasfileowner";

  static final boolean ADL_DEBUG_SET_LOCAL_USER_AS_OWNER_DEFAULT = false;
  static final long ADL_BLOCK_SIZE = 256 * 1024 * 1024;
  static final int ADL_REPLICATION_FACTOR = 1;
  static final String ADL_HADOOP_CLIENT_NAME = "hadoop-azure-datalake-";
  static final String ADL_HADOOP_CLIENT_VERSION =
      "2.0.0-SNAPSHOT";
  static final String ADL_EVENTS_TRACKING_CLUSTERNAME =
      "adl.events.tracking.clustername";

  static final String ADL_EVENTS_TRACKING_CLUSTERTYPE =
      "adl.events.tracking.clustertype";
  static final int DEFAULT_READ_AHEAD_BUFFER_SIZE = 4 * 1024 * 1024;
  static final int DEFAULT_WRITE_AHEAD_BUFFER_SIZE = 4 * 1024 * 1024;

  static final String LATENCY_TRACKER_KEY =
      "adl.enable.client.latency.tracker";
  static final boolean LATENCY_TRACKER_DEFAULT = true;

  static final String ADL_EXPERIMENT_POSITIONAL_READ_KEY =
      "adl.feature.experiment.positional.read.enable";
  static final boolean ADL_EXPERIMENT_POSITIONAL_READ_DEFAULT = true;

  static final String ADL_SUPPORT_ACL_BIT_IN_FSPERMISSION =
      "adl.feature.support.acl.bit";
  static final boolean ADL_SUPPORT_ACL_BIT_IN_FSPERMISSION_DEFAULT = true;

  static final String ADL_ENABLEUPN_FOR_OWNERGROUP_KEY =
      "adl.feature.ownerandgroup.enableupn";
  static final boolean ADL_ENABLEUPN_FOR_OWNERGROUP_DEFAULT = false;

  public static void addDeprecatedKeys() {
    Configuration.addDeprecations(new DeprecationDelta[]{
        new DeprecationDelta("dfs.adls.oauth2.access.token.provider.type",
            AZURE_AD_TOKEN_PROVIDER_TYPE_KEY),
        new DeprecationDelta("dfs.adls.oauth2.client.id",
            AZURE_AD_CLIENT_ID_KEY),
        new DeprecationDelta("dfs.adls.oauth2.refresh.token",
            AZURE_AD_REFRESH_TOKEN_KEY),
        new DeprecationDelta("dfs.adls.oauth2.refresh.url",
            AZURE_AD_REFRESH_URL_KEY),
        new DeprecationDelta("dfs.adls.oauth2.credential",
            AZURE_AD_CLIENT_SECRET_KEY),
        new DeprecationDelta("dfs.adls.oauth2.access.token.provider",
            AZURE_AD_TOKEN_PROVIDER_CLASS_KEY),
        new DeprecationDelta("adl.dfs.enable.client.latency.tracker",
            LATENCY_TRACKER_KEY)
    });
    Configuration.reloadExistingConfigurations();
  }

  private AdlConfKeys() {
  }
}
