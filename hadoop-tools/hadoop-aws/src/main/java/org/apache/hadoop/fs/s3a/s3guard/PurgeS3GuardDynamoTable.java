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

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.service.launcher.ServiceLaunchException;
import org.apache.hadoop.service.launcher.ServiceLauncher;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.s3guard.DumpS3GuardDynamoTable.serviceMain;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.PARENT;

/**
 * Purge the S3Guard table of a FileSystem from all entries related to
 * that table.
 * Will fail if there is no table, or the store is in auth mode.
 * <pre>
 *   hadoop org.apache.hadoop.fs.s3a.s3guard.PurgeS3GuardDynamoTable \
 *   -force s3a://example-bucket/
 * </pre>
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PurgeS3GuardDynamoTable
    extends AbstractS3GuardDynamoDBDiagnostic {

  private static final Logger LOG =
      LoggerFactory.getLogger(PurgeS3GuardDynamoTable.class);

  public static final String NAME = "PurgeS3GuardDynamoTable";

  /**
   * Name of the force option.
   */
  public static final String FORCE = "-force";

  /**
   * Usage message.
   */
  private static final String USAGE_MESSAGE = NAME
      + " [-force] <filesystem>";

  /**
   * Flag which actually triggers the delete.
   */
  private boolean force;

  private long filesFound;
  private long filesDeleted;

  public PurgeS3GuardDynamoTable(final String name) {
    super(name);
  }

  public PurgeS3GuardDynamoTable() {
    this(NAME);
  }

  public PurgeS3GuardDynamoTable(
      final S3AFileSystem filesystem,
      final DynamoDBMetadataStore store,
      final URI uri,
      final boolean force) {
    super(NAME, filesystem, store, uri);
    this.force = force;
  }

  /**
   * Bind to the argument list, including validating the CLI.
   * @throws Exception failure.
   */
  @Override
  protected void serviceStart() throws Exception {
    if (getStore() == null) {
      List<String> arg = getArgumentList(1, 2, USAGE_MESSAGE);
      String fsURI = arg.get(0);
      if (arg.size() == 2) {
        if (!arg.get(0).equals(FORCE)) {
          throw new ServiceLaunchException(LauncherExitCodes.EXIT_USAGE,
              USAGE_MESSAGE);
        }
        force = true;
        fsURI = arg.get(1);
      }
      bindFromCLI(fsURI);
    }
  }

  /**
   * Extract the host from the FS URI, then scan and
   * delete all entries from that bucket.
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

    LOG.info("Scanning for entries with prefix {} to delete from {}",
        prefix, ddbms);

    Iterable<DDBPathMetadata> entries =
        ddbms.wrapWithRetries(tableAccess.scanMetadata(builder));
    List<Path> list = new ArrayList<>();
    entries.iterator().forEachRemaining(e -> {
      if (!(e instanceof S3GuardTableAccess.VersionMarker)) {
        Path p = e.getFileStatus().getPath();
        String type = e.getFileStatus().isFile() ? "file" : "directory";
        boolean tombstone = e.isDeleted();
        if (tombstone) {
          type = "tombstone " + type;
        }
        LOG.info("{} {}", type, p);
        list.add(p);
      }
    });
    int count = list.size();
    filesFound = count;
    LOG.info("Found {} entries{}",
        count,
        (count == 0 ? " -nothing to purge": ""));
    if (count > 0) {
      if (force) {
        DurationInfo duration =
            new DurationInfo(LOG,
                "deleting %s entries from %s",
                count, ddbms.toString());
        // sending this in one by one for more efficient retries
        for (Path path: list) {
          ddbms.getInvoker()
              .retry("delete",
                  prefix,
                  true,
                  () -> tableAccess.delete(path));
        }
        duration.close();
        long durationMillis = duration.value();
        long timePerEntry = durationMillis / count;
        LOG.info("Time per entry: {} ms", timePerEntry);
        filesDeleted = count;
      } else {
        LOG.info("Delete process will only be executed when "
            + FORCE + " is set");
      }
    }
    return LauncherExitCodes.EXIT_SUCCESS;
  }

  /**
   * This is the Main entry point for the service launcher.
   *
   * Converts the arguments to a list, instantiates a instance of the class
   * then executes it.
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    try {
      serviceMain(Arrays.asList(args), new PurgeS3GuardDynamoTable());
    } catch (ExitUtil.ExitException e) {
      ExitUtil.terminate(e);
    }
  }

  /**
   * API Entry point to dump the metastore and S3 store world views
   * <p>
   * Both the FS and the store will be dumped: the store is scanned
   * before and after the sequence to show what changes were made to
   * the store during the list operation.
   * @param fs fs to dump. If null a store must be provided.
   * @param store store to dump (fallback to FS)
   * @param conf configuration to use (fallback to fs)
   * @param uri URI of store -only needed if FS is null.
   * @param force force the actual delete
   * @return (filesFound, filesDeleted)
   * @throws ExitUtil.ExitException failure.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static Pair<Long, Long> purgeStore(
      @Nullable final S3AFileSystem fs,
      @Nullable DynamoDBMetadataStore store,
      @Nullable Configuration conf,
      @Nullable URI uri,
      boolean force) throws ExitUtil.ExitException {
    ServiceLauncher<Service> serviceLauncher =
        new ServiceLauncher<>(NAME);

    if (conf == null) {
      conf = checkNotNull(fs, "No filesystem").getConf();
    }
    if (store == null) {
      store = (DynamoDBMetadataStore) checkNotNull(fs, "No filesystem")
          .getMetadataStore();
    }
    PurgeS3GuardDynamoTable purge = new PurgeS3GuardDynamoTable(fs,
        store,
        uri,
        force);
    ExitUtil.ExitException ex = serviceLauncher.launchService(
        conf,
        purge,
        Collections.emptyList(),
        false,
        true);
    if (ex != null && ex.getExitCode() != 0) {
      throw ex;
    }
    return Pair.of(purge.filesFound, purge.filesDeleted);
  }
}
