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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.service.launcher.ServiceLaunchException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.PARENT;

/**
 * Purge the S3Guard table of a FileSystem from all entries related to
 * that table.
 * Will fail if there is no table, or the store is in auth mode
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PurgeS3GuardTable extends AbstractS3GuardDiagnostic {

  private static final Logger LOG =
      LoggerFactory.getLogger(PurgeS3GuardTable.class);

  public static final String NAME = "PurgeS3GuardTable";

  public PurgeS3GuardTable(final String name) {
    super(name);
  }

  public PurgeS3GuardTable() {
    this(NAME);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (getStore() == null) {
      List<String> arguments = getArguments();
      checkNotNull(arguments, "No arguments");
      Preconditions.checkState(arguments.size() == 1,
          "Wrong number of arguments: %s", arguments.size());
      bindFromCLI(arguments.get(0));
    }
  }

  /**
   * Extract the host from the FS URI, then scan and
   * delete all entries from thtat bucket
   * @return the exit code.
   * @throws ServiceLaunchException on failure.
   * @throws IOException IO failure.
   */
  @Override
  public int execute() throws ServiceLaunchException, IOException {

    URI uri = getUri();
    String host = uri.getHost();
    String prefix = "/" + host + "/";
    DynamoDBMetadataStore ddbms = getStore();
    S3GuardTableAccess tableAccess = new S3GuardTableAccess(ddbms);
    ExpressionSpecBuilder builder = new ExpressionSpecBuilder();
    builder.withKeyCondition(
        ExpressionSpecBuilder.S(PARENT).beginsWith(prefix));

    Iterable<DDBPathMetadata> entries = tableAccess.scanMetadata(builder);
    List<Path> list = new ArrayList<>();
    entries.iterator().forEachRemaining(e -> {
      if (!(e instanceof S3GuardTableAccess.VersionMarker)) {
        Path p = e.getFileStatus().getPath();
        LOG.info("Deleting {}", p);
        list.add(p);
      }
    });
    LOG.info("Deleting {} entries", list.size());
    tableAccess.delete(list);
    return LauncherExitCodes.EXIT_SUCCESS;
  }
}
