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

package org.apache.hadoop.fs.contract;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This is a filesystem contract for any class that bonds to a filesystem
 * through the configuration.
 *
 * It looks for a definition of the test filesystem with the key
 * derived from "fs.contract.test.fs.%s" -if found the value
 * is converted to a URI and used to create a filesystem. If not -the
 * tests are not enabled
 */
public abstract class AbstractBondedFSContract extends AbstractFSContract {

  private static final Log LOG =
      LogFactory.getLog(AbstractBondedFSContract.class);

  /**
   * Pattern for the option for test filesystems from schema
   */
  public static final String FSNAME_OPTION = "test.fs.%s";

  /**
   * Constructor: loads the authentication keys if found

   * @param conf configuration to work with
   */
  protected AbstractBondedFSContract(Configuration conf) {
    super(conf);
  }

  private String fsName;
  private URI fsURI;
  private FileSystem filesystem;

  @Override
  public void init() throws IOException {
    super.init();
    //this test is only enabled if the test FS is present
    fsName = loadFilesystemName(getScheme());
    setEnabled(!fsName.isEmpty());
    if (isEnabled()) {
      try {
        fsURI = new URI(fsName);
        filesystem = FileSystem.get(fsURI, getConf());
      } catch (URISyntaxException e) {
        throw new IOException("Invalid URI " + fsName);
      } catch (IllegalArgumentException e) {
        throw new IOException("Unable to initialize filesystem " + fsName
            + ": " + e, e);
      }
    } else {
      LOG.info("skipping tests as FS name is not defined in "
              + getFilesystemConfKey());
    }
  }

  /**
   * Load the name of a test filesystem.
   * @param schema schema to look up
   * @return the filesystem name -or "" if none was defined
   */
  public String loadFilesystemName(String schema) {
    return getOption(String.format(FSNAME_OPTION, schema), "");
  }

  /**
   * Get the conf key for a filesystem
   */
  protected String getFilesystemConfKey() {
    return getConfKey(String.format(FSNAME_OPTION, getScheme()));
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return filesystem;
  }

  @Override
  public Path getTestPath() {
    Path path = new Path("/test");
    return path;
  }

  @Override
  public String toString() {
    return getScheme() +" Contract against " + fsName;
  }
}
