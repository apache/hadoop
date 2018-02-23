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

package org.apache.hadoop.mapreduce.v2.util;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for MR applications that parses distributed cache artifacts and
 * creates a map of LocalResources.
 */
@SuppressWarnings("deprecation")
@Private
@Unstable
class LocalResourceBuilder {
  public static final Logger LOG =
      LoggerFactory.getLogger(LocalResourceBuilder.class);

  private Configuration conf;
  private LocalResourceType type;
  private URI[] uris;
  private long[] timestamps;
  private long[] sizes;
  private boolean[] visibilities;
  private Map<String, Boolean> sharedCacheUploadPolicies;

  LocalResourceBuilder() {
  }

  void setConf(Configuration c) {
    this.conf = c;
  }

  void setType(LocalResourceType t) {
    this.type = t;
  }

  void setUris(URI[] u) {
    this.uris = u;
  }

  void setTimestamps(long[] t) {
    this.timestamps = t;
  }

  void setSizes(long[] s) {
    this.sizes = s;
  }

  void setVisibilities(boolean[] v) {
    this.visibilities = v;
  }

  void setSharedCacheUploadPolicies(Map<String, Boolean> policies) {
    this.sharedCacheUploadPolicies = policies;
  }

  void createLocalResources(Map<String, LocalResource> localResources)
      throws IOException {

    if (uris != null) {
      // Sanity check
      if ((uris.length != timestamps.length) || (uris.length != sizes.length) ||
          (uris.length != visibilities.length)) {
        throw new IllegalArgumentException("Invalid specification for " +
            "distributed-cache artifacts of type " + type + " :" +
            " #uris=" + uris.length +
            " #timestamps=" + timestamps.length +
            " #visibilities=" + visibilities.length
            );
      }

      for (int i = 0; i < uris.length; ++i) {
        URI u = uris[i];
        Path p = new Path(u);
        FileSystem remoteFS = p.getFileSystem(conf);
        String linkName = null;

        if (p.getName().equals(DistributedCache.WILDCARD)) {
          p = p.getParent();
          linkName = p.getName() + Path.SEPARATOR + DistributedCache.WILDCARD;
        }

        p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory()));

        // If there's no wildcard, try using the fragment for the link
        if (linkName == null) {
          linkName = u.getFragment();

          // Because we don't know what's in the fragment, we have to handle
          // it with care.
          if (linkName != null) {
            Path linkPath = new Path(linkName);

            if (linkPath.isAbsolute()) {
              throw new IllegalArgumentException("Resource name must be "
                  + "relative");
            }

            linkName = linkPath.toUri().getPath();
          }
        } else if (u.getFragment() != null) {
          throw new IllegalArgumentException("Invalid path URI: " + p +
              " - cannot contain both a URI fragment and a wildcard");
        }

        // If there's no wildcard or fragment, just link to the file name
        if (linkName == null) {
          linkName = p.getName();
        }

        LocalResource orig = localResources.get(linkName);
        if(orig != null && !orig.getResource().equals(URL.fromURI(p.toUri()))) {
          throw new InvalidJobConfException(
              getResourceDescription(orig.getType()) + orig.getResource()
                  +
              " conflicts with " + getResourceDescription(type) + u);
        }
        Boolean sharedCachePolicy = sharedCacheUploadPolicies.get(u.toString());
        sharedCachePolicy =
            sharedCachePolicy == null ? Boolean.FALSE : sharedCachePolicy;
        localResources.put(linkName, LocalResource.newInstance(URL.fromURI(p
            .toUri()), type, visibilities[i] ? LocalResourceVisibility.PUBLIC
                : LocalResourceVisibility.PRIVATE,
            sizes[i], timestamps[i], sharedCachePolicy));
      }
    }
  }

  private static String getResourceDescription(LocalResourceType type) {
    if (type == LocalResourceType.ARCHIVE
        || type == LocalResourceType.PATTERN) {
      return "cache archive (" + MRJobConfig.CACHE_ARCHIVES + ") ";
    }
    return "cache file (" + MRJobConfig.CACHE_FILES + ") ";
  }
}
