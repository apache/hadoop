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

package org.apache.hadoop.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * GlobbedCopyListing implements the CopyListing interface, to create the copy
 * listing-file by "globbing" all specified source paths (wild-cards and all.)
 */
public class GlobbedCopyListing extends CopyListing {
  private static final Log LOG = LogFactory.getLog(GlobbedCopyListing.class);

  private final CopyListing simpleListing;
  /**
   * Constructor, to initialize the configuration.
   * @param configuration The input Configuration object.
   * @param credentials Credentials object on which the FS delegation tokens are cached. If null
   * delegation token caching is skipped
   */
  public GlobbedCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
    simpleListing = new SimpleCopyListing(getConf(), credentials) ;
  }

  /** {@inheritDoc} */
  @Override
  protected void validatePaths(DistCpOptions options)
      throws IOException, InvalidInputException {
  }

  /**
   * Implementation of CopyListing::buildListing().
   * Creates the copy listing by "globbing" all source-paths.
   * @param pathToListingFile The location at which the copy-listing file
   *                           is to be created.
   * @param options Input Options for DistCp (indicating source/target paths.)
   * @throws IOException
   */
  @Override
  public void doBuildListing(Path pathToListingFile,
                             DistCpOptions options) throws IOException {

    List<Path> globbedPaths = new ArrayList<Path>();
    if (options.getSourcePaths().isEmpty()) {
      throw new InvalidInputException("Nothing to process. Source paths::EMPTY");  
    }

    for (Path p : options.getSourcePaths()) {
      FileSystem fs = p.getFileSystem(getConf());
      FileStatus[] inputs = fs.globStatus(p);

      if(inputs != null && inputs.length > 0) {
        for (FileStatus onePath: inputs) {
          globbedPaths.add(onePath.getPath());
        }
      } else {
        throw new InvalidInputException(p + " doesn't exist");        
      }
    }

    DistCpOptions optionsGlobbed = new DistCpOptions(options);
    optionsGlobbed.setSourcePaths(globbedPaths);
    simpleListing.buildListing(pathToListingFile, optionsGlobbed);
  }

  /** {@inheritDoc} */
  @Override
  protected long getBytesToCopy() {
    return simpleListing.getBytesToCopy();
  }

  /** {@inheritDoc} */
  @Override
  protected long getNumberOfPaths() {
    return simpleListing.getNumberOfPaths();
  }

}
