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
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.service.launcher.AbstractLaunchableService;
import org.apache.hadoop.service.launcher.ServiceLaunchException;

import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_FAIL;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;

/**
 * Entry point for S3Guard diagnostics operations against DynamoDB tables.
 */
public class AbstractS3GuardDynamoDBDiagnostic
    extends AbstractLaunchableService {

  private S3AFileSystem filesystem;

  private DynamoDBMetadataStore store;

  private URI uri;

  private List<String> arguments;

  /**
   * Constructor.
   * @param name entry point name.
   */
  public AbstractS3GuardDynamoDBDiagnostic(final String name) {
    super(name);
  }

  /**
   * Constructor. If the store is set then that is the store for the operation,
   * otherwise the filesystem's binding is used instead.
   * @param name entry point name.
   * @param filesystem filesystem
   * @param store optional metastore.
   * @param uri URI. Must be set if filesystem == null.
   */
  public AbstractS3GuardDynamoDBDiagnostic(
      final String name,
      @Nullable final S3AFileSystem filesystem,
      @Nullable final DynamoDBMetadataStore store,
      @Nullable final URI uri) {
    super(name);
    this.store = store;
    this.filesystem = filesystem;
    if (store == null) {
      require(filesystem != null, "No filesystem or URI");
      bindStore(filesystem);
    }
    if (uri == null) {
      require(filesystem != null, "No filesystem or URI");
      setUri(filesystem.getUri());
    } else {
      setUri(uri);
    }
  }

  /**
   * Require a condition to hold, otherwise an exception is thrown.
   * @param condition condition to be true
   * @param error text on failure.
   * @throws ServiceLaunchException if the condition is not met
   */
  protected static void require(boolean condition, String error) {
    if (!condition) {
      throw failure(error);
    }
  }

  /**
   * Generate a failure exception for throwing.
   * @param message message
   * @param ex optional nested exception.
   * @return an exception to throw
   */
  protected static ServiceLaunchException failure(String message,
      Throwable ex) {
    return new ServiceLaunchException(EXIT_FAIL, message, ex);
  }

  /**
   * Generate a failure exception for throwing.
   * @param message message
   * @return an exception to throw
   */
  protected static ServiceLaunchException failure(String message) {
    return new ServiceLaunchException(EXIT_FAIL, message);
  }

  @Override
  public Configuration bindArgs(final Configuration config,
      final List<String> args)
      throws Exception {
    this.arguments = args;
    return super.bindArgs(config, args);
  }

  /**
   * Get the argument list.
   * @return the argument list.
   */
  protected List<String> getArguments() {
    return arguments;
  }

  /**
   * Bind to the store from a CLI argument.
   * @param fsURI filesystem URI
   * @throws IOException failure
   */
  protected void bindFromCLI(String fsURI)
      throws IOException {
    Configuration conf = getConfig();
    setUri(fsURI);
    FileSystem fs = FileSystem.get(getUri(), conf);
    require(fs instanceof S3AFileSystem,
        "Not an S3A Filesystem:  " + fsURI);
    filesystem = (S3AFileSystem) fs;
    bindStore(filesystem);
    setUri(fs.getUri());
  }

  /**
   * Binds the {@link #store} field to the metastore of
   * the filesystem -which must have a DDB metastore.
   * @param fs filesystem to bind the store to.
   */
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

  /**
   * Get the list of arguments, after validating the list size.
   * @param argMin minimum number of entries.
   * @param argMax maximum number of entries.
   * @param usage Usage message.
   * @return the argument list, which will be in the range.
   * @throws ServiceLaunchException if the argument list is not valid.
   */
  protected List<String> getArgumentList(final int argMin,
      final int argMax,
      final String usage) {
    List<String> arg = getArguments();
    if (arg == null || arg.size() < argMin || arg.size() > argMax) {
      // no arguments: usage message
      throw new ServiceLaunchException(EXIT_USAGE, usage);
    }
    return arg;
  }
}
