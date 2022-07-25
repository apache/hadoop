/**
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
package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegationTokenUtil {
  public static final String HADOOP_TOKEN_FILE_LOCATION =
          "HADOOP_TOKEN_FILE_LOCATION";

  static final Logger LOG = LoggerFactory.getLogger(
      DelegationTokenUtil.class);

  private DelegationTokenUtil() {
  }

  public static synchronized Credentials readDelegationTokens(Configuration conf)
        throws IOException {
    String fileLocation = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
    if (fileLocation != null) {
      // Load the token storage file and put all of the tokens into the
      // user. Don't use the FileSystem API for reading since it has a lock
      // cycle (HADOOP-9212).
      File source = new File(fileLocation);
      Credentials creds = Credentials.readTokenStorageFile(
          source, conf);
      LOG.info("Loaded {} tokens", creds.numberOfTokens());
      return creds;
    }
    return null;
  }
}
