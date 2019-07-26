/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

/**
 * The class is used to represent a resource of MapReduce job.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MRResource {

  private MRResourceType resourceType;
  private MRResourceVisibility resourceVisibility;
  private String resourceUriStr;

  public MRResource(
      MRResourceType type, String path, MRResourceVisibility visibility) {
    resourceType = type;
    resourceUriStr = path;
    resourceVisibility = visibility;
  }

  public MRResourceType getResourceType() {
    return resourceType;
  }

  public void setResourceType(MRResourceType resourceType) {
    this.resourceType = resourceType;
  }

  public MRResourceVisibility getResourceVisibility() {
    return resourceVisibility;
  }

  public LocalResourceVisibility getYarnLocalResourceVisibility() {
    return MRResourceVisibility.getYarnLocalResourceVisibility(
        resourceVisibility);
  }

  public void setResourceVisibility(MRResourceVisibility resourceVisibility) {
    this.resourceVisibility = resourceVisibility;
  }

  public String getResourcePathStr() {
    return resourceUriStr;
  }

  public void setResourceUriStr(String resourceUriStr) {
    this.resourceUriStr = resourceUriStr;
  }

  public Path getResourceSubmissionParentDir(Path jobSubmissionDir) {
    if (resourceType.getResourceParentDirName().isEmpty()) {
      return new Path(jobSubmissionDir,
          resourceVisibility.getDirName());
    } else {
      return new Path(jobSubmissionDir,
          resourceVisibility.getDirName()
              + "/" + resourceType.getResourceParentDirName());
    }
  }

  public URI getResourceUri(Configuration conf) throws IOException {
    Path path = new Path(resourceUriStr);
    URI uri = path.toUri();
    if (uri == null
        || uri.getScheme() == null
        || uri.getScheme().equals("file")) {
      path = FileSystem.getLocal(conf).makeQualified(path);
    }
    try {
      return new URI(path.toString());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Error parsing files argument."
        + " Argument must be a valid URI: " + toString(), e);
    }
  }

  @Override
  public String toString() {
    return "MRResource resourceType:" + resourceType + ", resourceVisibility:"
        + resourceVisibility + ", resource path:" + resourceUriStr;
  }
}
