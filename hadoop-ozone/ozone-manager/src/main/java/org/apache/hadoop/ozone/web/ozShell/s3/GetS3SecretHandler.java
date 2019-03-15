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
package org.apache.hadoop.ozone.web.ozShell.s3;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine.Command;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;

/**
 * Executes getsecret calls.
 */
@Command(name = "getsecret",
    description = "Returns s3 secret for current user")
public class GetS3SecretHandler extends Handler {

  public static final String OZONE_GETS3SECRET_ERROR = "This command is not" +
      " supported in unsecure clusters.";
  /**
   * Executes getS3Secret.
   */
  @Override
  public Void call() throws Exception {
    OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();
    OzoneClient client =
        new OzoneAddress().createClient(ozoneConfiguration);

    // getS3Secret works only with secured clusters
    if (ozoneConfiguration.getBoolean(OZONE_SECURITY_ENABLED_KEY, false)) {
      System.out.println(
          client.getObjectStore().getS3Secret(
              UserGroupInformation.getCurrentUser().getUserName()
          ).toString()
      );
    } else {
      // log a warning message for unsecured cluster
      System.out.println(OZONE_GETS3SECRET_ERROR);
    }

    return null;
  }
}
