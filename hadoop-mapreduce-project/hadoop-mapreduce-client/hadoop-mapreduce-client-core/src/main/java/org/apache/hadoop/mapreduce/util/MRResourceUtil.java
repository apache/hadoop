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

package org.apache.hadoop.mapreduce.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MRResource;
import org.apache.hadoop.mapreduce.MRResourceType;
import org.apache.hadoop.mapreduce.MRResourceVisibility;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ResourceWithVisibilitySetting;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

import static org.apache.hadoop.mapreduce.MRResourceType.ARCHIVE;
import static org.apache.hadoop.mapreduce.MRResourceType.FILE;
import static org.apache.hadoop.mapreduce.MRResourceType.JOBJAR;
import static org.apache.hadoop.mapreduce.MRResourceType.LIBJAR;

/**
 * MapReduce utility class.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class MRResourceUtil {

  private MRResourceUtil() {}

  /**
   * Generate resource from config string. Config format should like
   *  "$path" or "$path:${visibility}
   *  Visibility should be one fo following value
   *  {@link MRResourceVisibility#PUBLIC},
   *  {@link MRResourceVisibility#APPLICATION},
   *  {@link MRResourceVisibility#PRIVATE}"
   * @param confString
   * @param resourceType
   * @param defaultVisibility
   * @return
   */
  public static MRResource getResourceFromOptionStr(
      String confString, MRResourceType resourceType,
      MRResourceVisibility defaultVisibility) {
    ResourceWithVisibilitySetting resource =
        ResourceWithVisibilitySetting.deserialize(confString);
    MRResourceVisibility visibility =
        MRResourceVisibility.getVisibility(
            resource.getVisibilitySettings());
    visibility = visibility == null ? defaultVisibility : visibility;
    return new MRResource(resourceType, resource.getPathStr(), visibility);
  }

  /**
   * Get resource specified by -libjars, -files, -archives and jobjar.
   * @param resourceType
   * @param configuration
   * @return
   */
  public static List<MRResource> getCmdConfiguredResourceFromMRConfig(
      MRResourceType resourceType, Configuration configuration) {
    MRResourceVisibility defaultVisibility =
        getVisibilityFromMRConfig(resourceType, configuration);
    switch (resourceType) {
    case JOBJAR:
      return createResourceCollectionsInternal(
          JOBJAR, defaultVisibility, JobContext.JAR, configuration);
    case ARCHIVE:
      return createResourceCollectionsInternal(
          ARCHIVE, defaultVisibility,
          GenericOptionsParser.TMP_ARCHIVES_CONF_KEY, configuration);
    case LIBJAR:
      return createResourceCollectionsInternal(
          LIBJAR, defaultVisibility,
          GenericOptionsParser.TMP_LIBJARS_CONF_KEY, configuration);
    case FILE:
      return createResourceCollectionsInternal(
          FILE, defaultVisibility,
          GenericOptionsParser.TMP_FILES_CONF_KEY, configuration);
    default:
      throw new IllegalArgumentException(
          "Unkown resource type:" + resourceType);
    }
  }

  /**
   * Get resources specified from cmd line and resources that have been
   * programmatically specified for the shared cache via the Job API.
   * @return
   */
  public static List<MRResource> getResourceFromMRConfig(
      MRResourceType resourceType, Configuration configuration) {
    List<MRResource> resourceList =
        getCmdConfiguredResourceFromMRConfig(resourceType, configuration);
    if (resourceType != JOBJAR) {
      // We might have resources not passed in by CMD configuration if it's not
      // job jar.
      resourceList.addAll(
          getSharedCacheResourceFromMRConfig(
              resourceType, configuration));
    }
    return resourceList;
  }

  /**
   * Get resources that have been programmatically specified for the shared
   *  cache via the Job API.
   * @param resourceType
   * @param configuration
   * @return
   */
  public static List<MRResource> getSharedCacheResourceFromMRConfig(
      MRResourceType resourceType, Configuration configuration) {
    MRResourceVisibility defaultVisibility =
        getVisibilityFromMRConfig(resourceType, configuration);
    switch (resourceType) {
    case ARCHIVE:
      return createResourceCollectionsInternal(
          ARCHIVE, defaultVisibility,
          MRJobConfig.ARCHIVES_FOR_SHARED_CACHE, configuration);
    case LIBJAR:
      return createResourceCollectionsInternal(
          LIBJAR, defaultVisibility,
          MRJobConfig.FILES_FOR_CLASSPATH_AND_SHARED_CACHE, configuration);
    case FILE:
      return createResourceCollectionsInternal(
          FILE, defaultVisibility,
          MRJobConfig.FILES_FOR_SHARED_CACHE, configuration);
    case JOBJAR:
      // There are no shared cached resource added to configuration
    default:
      throw new IllegalArgumentException(
          "Unkown resource type:" + resourceType);
    }
  }

  public static MRResource getJobJar(Configuration configuration) {
    Collection<MRResource> jobJarResource =
        getCmdConfiguredResourceFromMRConfig(
            MRResourceType.JOBJAR, configuration);
    if (jobJarResource == null || jobJarResource.isEmpty()) {
      return null;
    }
    return jobJarResource.iterator().next();
  }

  private static List<MRResource> createResourceCollectionsInternal(
      MRResourceType resourceType, MRResourceVisibility visibility,
      String confKey, Configuration configuration) {
    List<MRResource> resourceList = new ArrayList<MRResource>();
    Collection<String> resourceStrCollection =
        configuration.getStringCollection(confKey);
    for (String resourceStr : resourceStrCollection) {
      MRResource resource =  getResourceFromOptionStr(
          resourceStr, resourceType, visibility);
      resourceList.add(resource);
    }
    return resourceList;
  }

  public static MRResourceVisibility getVisibilityFromMRConfig(
      MRResourceType resourceType, Configuration conf) {
    MRResourceVisibility visibility = MRResourceVisibility.APPLICATION;
    switch (resourceType) {
    case JOBJAR:
      visibility = getResourceVisibility(
          conf, MRJobConfig.JOBJAR_VISIBILITY,
          MRJobConfig.JOBJAR_VISIBILITY_DEFAULT);
      break;
    case LIBJAR:
      visibility = getResourceVisibility(
          conf, MRJobConfig.LIBJARS_VISIBILITY,
          MRJobConfig.LIBJARS_VISIBILITY_DEFAULT);
      break;
    case FILE:
      visibility = getResourceVisibility(
          conf, MRJobConfig.FILES_VISIBILITY,
          MRJobConfig.FILES_VISIBILITY_DEFAULT);
      break;
    case ARCHIVE:
      visibility = getResourceVisibility(
          conf, MRJobConfig.ARCHIEVS_VISIBILITY,
          MRJobConfig.ARCHIEVES_VISIBILITY_DEFAULT);
      break;
    default:
      break;
    }
    return visibility;
  }

  public static MRResourceVisibility getResourceVisibility(
      Configuration conf, String key, MRResourceVisibility defaultVal) {
    String visibilityStr = conf.get(key);
    if (visibilityStr == null) {
      return defaultVal;
    }
    return MRResourceVisibility.getVisibility(visibilityStr);
  }

  /**
   * This method should be called after job jar is uploaded to HDFS.
   * @param conf
   * @return
   */
  public static LocalResourceVisibility getJobJarYarnLocalVisibility(
      Configuration conf) {
    String visibilityStr = conf.get(
        MRJobConfig.CACHE_JOBJAR_VISIBILITY);
    if (visibilityStr == null) {
      // The visibility string will be null before 2.9 as it uses
      // {@link MRJobConf.JOBJAR_VISIBILITY} to determined jar visibility.
      // We make MRJobConf.JOBJAR_VISIBILITY user facing only and
      // use CACHE_JOBJAR_VISIBILITY to determine job jar visibility after 2.9.
      String oldFormatVis = conf.get(MRJobConfig.JOBJAR_VISIBILITY);
      if (oldFormatVis != null) {
        return oldFormatVis.equalsIgnoreCase("true") ?
            LocalResourceVisibility.PUBLIC :
            LocalResourceVisibility.APPLICATION;
      }
      return MRResourceVisibility.getYarnLocalResourceVisibility(
          MRJobConfig.JOBJAR_VISIBILITY_DEFAULT);
    }
    return MRResourceVisibility.getYarnLocalResourceVisibility(
        MRResourceVisibility.getVisibility(visibilityStr));
  }

  public static LocalResourceVisibility getFilesYarnLocalVisibility(
      Configuration conf) {
    return MRResourceVisibility.getYarnLocalResourceVisibility(
        getResourceVisibility(
            conf, MRJobConfig.FILES_VISIBILITY,
            MRJobConfig.FILES_VISIBILITY_DEFAULT));
  }

  public static LocalResourceVisibility getArchivesYarnLocalVisibility(
      Configuration conf) {
    return MRResourceVisibility.getYarnLocalResourceVisibility(
        getResourceVisibility(
            conf, MRJobConfig.ARCHIEVS_VISIBILITY,
            MRJobConfig.ARCHIEVES_VISIBILITY_DEFAULT));
  }

  public static LocalResourceVisibility getLibjarsYarnLocalVisibility(
      Configuration conf) {
    return MRResourceVisibility.getYarnLocalResourceVisibility(
        getResourceVisibility(
            conf, MRJobConfig.LIBJARS_VISIBILITY,
            MRJobConfig.LIBJARS_VISIBILITY_DEFAULT));
  }
}
