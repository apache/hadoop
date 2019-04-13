/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmOps;
import org.apache.hadoop.hdds.scm.safemode.Precheck;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * SCM utility class.
 */
public final class ScmUtils {
  private static final Logger LOG = LoggerFactory
      .getLogger(ScmUtils.class);

  private ScmUtils() {
  }

  /**
   * Perform all prechecks for given scm operation.
   *
   * @param operation
   * @param preChecks prechecks to be performed
   */
  public static void preCheck(ScmOps operation, Precheck... preChecks)
      throws SCMException {
    for (Precheck preCheck : preChecks) {
      preCheck.check(operation);
    }
  }

  public static File getDBPath(Configuration conf, String dbDirectory) {
    final File dbDirPath =
        ServerUtils.getDirectoryFromConfig(conf, dbDirectory, "OM");
    if (dbDirPath != null) {
      return dbDirPath;
    }

    LOG.warn("{} is not configured. We recommend adding this setting. "
            + "Falling back to {} instead.", dbDirectory,
        HddsConfigKeys.OZONE_METADATA_DIRS);
    return ServerUtils.getOzoneMetaDirPath(conf);
  }
}
