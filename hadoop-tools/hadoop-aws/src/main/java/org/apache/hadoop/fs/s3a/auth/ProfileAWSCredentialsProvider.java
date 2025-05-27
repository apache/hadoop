/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.auth;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;

import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ProfileAWSCredentialsProvider extends AbstractAWSCredentialProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ProfileAWSCredentialsProvider.class);

  public static final String NAME
      = "org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider";

  /** Conf setting for credentials file path.*/
  public static final String PROFILE_FILE = "fs.s3a.auth.profile.file";

  /** Conf setting for profile name.*/
  public static final String PROFILE_NAME = "fs.s3a.auth.profile.name";

  /** Environment variable for credentials file path.*/
  public static final String CREDENTIALS_FILE_ENV = "AWS_SHARED_CREDENTIALS_FILE";
  /** Environment variable for profile name.*/
  public static final String PROFILE_ENV = "AWS_PROFILE";

  private final ProfileCredentialsProvider pcp;

  private static Path getCredentialsPath(Configuration conf) {
    String credentialsFile = conf.get(PROFILE_FILE, null);
    if (credentialsFile == null) {
      credentialsFile = SystemUtils.getEnvironmentVariable(CREDENTIALS_FILE_ENV, null);
      if (credentialsFile != null) {
        LOG.debug("Fetched credentials file path from environment variable");
      }
    } else {
      LOG.debug("Fetched credentials file path from conf");
    }
    if (credentialsFile == null) {
      LOG.debug("Using default credentials file path");
      return FileSystems.getDefault().getPath(SystemUtils.getUserHome().getPath(),
        ".aws", "credentials");
    } else {
      return FileSystems.getDefault().getPath(credentialsFile);
    }
  }

  private static String getCredentialsName(Configuration conf) {
    String profileName = conf.get(PROFILE_NAME, null);
    if (profileName == null) {
      profileName = SystemUtils.getEnvironmentVariable(PROFILE_ENV, null);
      if (profileName == null) {
        profileName = "default";
        LOG.debug("Using default profile name");
      } else {
        LOG.debug("Fetched profile name from environment variable");
      }
    } else {
      LOG.debug("Fetched profile name from conf");
    }
    return profileName;
  }

  public ProfileAWSCredentialsProvider(URI uri, Configuration conf) {
    super(uri, conf);
    ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder();
    builder.profileName(getCredentialsName(conf))
            .profileFile(ProfileFile.builder()
            .content(getCredentialsPath(conf))
            .type(ProfileFile.Type.CREDENTIALS)
            .build());
    pcp = builder.build();
  }

  public AwsCredentials resolveCredentials() {
    return pcp.resolveCredentials();
  }
}
