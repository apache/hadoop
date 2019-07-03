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
import java.net.URISyntaxException;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.service.launcher.AbstractLaunchableService;
import org.apache.hadoop.service.launcher.LauncherExitCodes;
import org.apache.hadoop.service.launcher.ServiceLaunchException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AbstractS3GuardDiagnostic extends AbstractLaunchableService {

  private S3AFileSystem filesystem;

  private DynamoDBMetadataStore store;

  private URI uri;

  private List<String> arguments;

  public AbstractS3GuardDiagnostic(final String name) {
    super(name);
  }

  public AbstractS3GuardDiagnostic(final String name,
      final S3AFileSystem filesystem,
      final DynamoDBMetadataStore store,
      final URI uri) {
    super(name);
    this.store = store;
    this.filesystem = filesystem;
    if (uri == null) {
      checkArgument(filesystem != null, "No filesystem or URI");
        // URI always gets a trailing /
        setUri(filesystem.getUri().toString());
    } else {
      setUri(uri);
    }
    if (store == null) {
      bindStore(filesystem);
    }
  }

  private static void require(boolean condition, String error) {
    if (!condition) {
      throw fail(error);
    }
  }

  private static ServiceLaunchException fail(String message, Throwable ex) {
    return new ServiceLaunchException(LauncherExitCodes.EXIT_FAIL, message, ex);
  }

  private static ServiceLaunchException fail(String message) {
    return new ServiceLaunchException(LauncherExitCodes.EXIT_FAIL, message);
  }

  @Override
  public Configuration bindArgs(final Configuration config,
      final List<String> args)
      throws Exception {
    this.arguments = args;
    return super.bindArgs(config, args);
  }

  public List<String> getArguments() {
    return arguments;
  }

  protected void bindFromCLI(String fsURI )
      throws IOException, URISyntaxException {
    Configuration conf = getConfig();
    setUri(fsURI);
    FileSystem fs = FileSystem.get(getUri(), conf);
    require(fs instanceof S3AFileSystem,
        "Not an S3A Filesystem:  " + fsURI);
    filesystem = (S3AFileSystem) fs;
    bindStore(filesystem);

  }

  private void bindStore(final S3AFileSystem fs) {
    require(fs.hasMetadataStore(),
        "Filesystem has no metadata store: " + fs.getUri());
    MetadataStore ms = fs.getMetadataStore();
    require(ms instanceof DynamoDBMetadataStore,
        "Filesystem " + fs.getUri()
            + " does not have a DynamoDB metadata store:  " + ms);
    store = (DynamoDBMetadataStore) ms;
  }

  protected DynamoDBMetadataStore getStore() {
    return store;
  }

  public S3AFileSystem getFilesystem() {
    return filesystem;
  }

  public URI getUri() {
    return uri;
  }

  public void setUri(final URI uri) {
    String fsURI = uri.toString();
    if (!fsURI.endsWith("/")) {
      setUri(fsURI);
    } else {
      this.uri = uri;
    }
  }

  /**
   * Set the URI from a string; will add a "/" if needed.
   * @param fsURI filesystem URI.
   * @throws RuntimeException if the fsURI parameter is not a valid URI.
   */
  public void setUri(String fsURI) {
    if (fsURI != null) {
      if (!fsURI.endsWith("/")) {
        fsURI += "/";
      }
      try {
        setUri(new URI(fsURI));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
