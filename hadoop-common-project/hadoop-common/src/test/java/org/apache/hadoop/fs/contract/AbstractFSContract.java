/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Class representing a filesystem contract that a filesystem
 * implementation is expected implement.
 *
 * Part of this contract class is to allow FS implementations to
 * provide specific opt outs and limits, so that tests can be
 * skip unsupported features (e.g. case sensitivity tests),
 * dangerous operations (e.g. trying to delete the root directory),
 * and limit filesize and other numeric variables for scale tests
 */
public abstract class AbstractFSContract extends Configured {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractFSContract.class);

  private boolean enabled = true;


  /**
   * Constructor: loads the authentication keys if found
   * @param conf configuration to work with
   */
  protected AbstractFSContract(Configuration conf) {
    super(conf);
    if (maybeAddConfResource(ContractOptions.CONTRACT_OPTIONS_RESOURCE)) {
      LOG.debug("Loaded authentication keys from {}", ContractOptions.CONTRACT_OPTIONS_RESOURCE);
    } else {
      LOG.debug("Not loaded: {}", ContractOptions.CONTRACT_OPTIONS_RESOURCE);
    }
  }

  /**
   * Any initialisation logic can go here
   * @throws IOException IO problems
   */
  public void init() throws IOException {

  }

  /**
   * Add a configuration resource to this instance's configuration
   * @param resource resource reference
   * @throws AssertionError if the resource was not found.
   */
  protected void addConfResource(String resource) {
    boolean found = maybeAddConfResource(resource);
    Assert.assertTrue("Resource not found " + resource, found);
  }

  /**
   * Add a configuration resource to this instance's configuration,
   * return true if the resource was found
   * @param resource resource reference
   */
  protected boolean maybeAddConfResource(String resource) {
    URL url = this.getClass().getClassLoader().getResource(resource);
    boolean found = url != null;
    if (found) {
      getConf().addResource(resource);
    }
    return found;
  }


  /**
   * Get the FS from a URI. The default implementation just retrieves
   * it from the norrmal FileSystem factory/cache, with the local configuration
   * @param uri URI of FS
   * @return the filesystem
   * @throws IOException IO problems
   */
  public FileSystem getFileSystem(URI uri) throws IOException {
    return FileSystem.get(uri, getConf());
  }

  /**
   * Get the filesystem for these tests
   * @return the test fs
   * @throws IOException IO problems
   */
  public abstract FileSystem getTestFileSystem() throws IOException;

  /**
   * Get the scheme of this FS
   * @return the scheme this FS supports
   */
  public abstract String getScheme();

  /**
   * Return the path string for tests, e.g. <code>file:///tmp</code>
   * @return a path in the test FS
   */
  public abstract Path getTestPath();

  /**
   * Boolean to indicate whether or not the contract test are enabled
   * for this test run.
   * @return true if the tests can be run.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Boolean to indicate whether or not the contract test are enabled
   * for this test run.
   * @param enabled flag which must be true if the tests can be run.
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Query for a feature being supported. This may include a probe for the feature
   *
   * @param feature feature to query
   * @param defval default value
   * @return true if the feature is supported
   */
  public boolean isSupported(String feature, boolean defval) {
    return getConf().getBoolean(getConfKey(feature), defval);
  }

  /**
   * Query for a feature's limit. This may include a probe for the feature
   *
   * @param feature feature to query
   * @param defval default value
   * @return true if the feature is supported
   */
  public int getLimit(String feature, int defval) {
    return getConf().getInt(getConfKey(feature), defval);
  }

  public String getOption(String feature, String defval) {
    return getConf().get(getConfKey(feature), defval);
  }

  /**
   * Build a configuration key
   * @param feature feature to query
   * @return the configuration key base with the feature appended
   */
  public String getConfKey(String feature) {
    return ContractOptions.FS_CONTRACT_KEY + feature;
  }

  /**
   * Create a URI off the scheme
   * @param path path of URI
   * @return a URI
   * @throws IOException if the URI could not be created
   */
  protected URI toURI(String path) throws IOException {
    try {
      return new URI(getScheme(),path, null);
    } catch (URISyntaxException e) {
      throw new IOException(e.toString() + " with " + path, e);
    }
  }

  @Override
  public String toString() {
    return "FSContract for " + getScheme();
  }

}
