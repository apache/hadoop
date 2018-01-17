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

package org.apache.hadoop.yarn.util.timeline;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;

/**
 * The helper class for the timeline module.
 * 
 */
@Public
@Evolving
public class TimelineUtils {

  public static final String FLOW_NAME_TAG_PREFIX = "TIMELINE_FLOW_NAME_TAG";
  public static final String FLOW_VERSION_TAG_PREFIX =
      "TIMELINE_FLOW_VERSION_TAG";
  public static final String FLOW_RUN_ID_TAG_PREFIX =
      "TIMELINE_FLOW_RUN_ID_TAG";
  public final static String DEFAULT_FLOW_VERSION = "1";

  private static ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);
  }

  /**
   * Serialize a POJO object into a JSON string not in a pretty format
   * 
   * @param o
   *          an object to serialize
   * @return a JSON string
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonGenerationException
   */
  public static String dumpTimelineRecordtoJSON(Object o)
      throws JsonGenerationException, JsonMappingException, IOException {
    return dumpTimelineRecordtoJSON(o, false);
  }

  /**
   * Serialize a POJO object into a JSON string
   * 
   * @param o
   *          an object to serialize
   * @param pretty
   *          whether in a pretty format or not
   * @return a JSON string
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonGenerationException
   */
  public static String dumpTimelineRecordtoJSON(Object o, boolean pretty)
      throws JsonGenerationException, JsonMappingException, IOException {
    if (pretty) {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o);
    } else {
      return mapper.writeValueAsString(o);
    }
  }

  /**
   * Returns whether the timeline service is enabled via configuration.
   *
   * @param conf the configuration
   * @return whether the timeline service is enabled.
   */
  public static boolean timelineServiceEnabled(Configuration conf) {
    return conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED);
  }

  /**
   * Returns the timeline service version. It does not check whether the
   * timeline service itself is enabled.
   *
   * @param conf the configuration
   * @return the timeline service version as a float.
   */
  public static float getTimelineServiceVersion(Configuration conf) {
    return conf.getFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_VERSION);
  }

  /**
   * Returns whether the timeline service v.1.5 is enabled by default via
   * configuration.
   *
   * @param conf the configuration
   * @return whether the timeline service v.1.5 is enabled. V.1.5 refers to a
   * version equal to 1.5.
   */
  public static boolean timelineServiceV1_5Enabled(Configuration conf) {
    return timelineServiceEnabled(conf) &&
        Math.abs(getTimelineServiceVersion(conf) - 1.5) < 0.00001;
  }

  public static TimelineAbout createTimelineAbout(String about) {
    TimelineAbout tsInfo = new TimelineAbout(about);
    tsInfo.setHadoopBuildVersion(VersionInfo.getBuildVersion());
    tsInfo.setHadoopVersion(VersionInfo.getVersion());
    tsInfo.setHadoopVersionBuiltOn(VersionInfo.getDate());
    tsInfo.setTimelineServiceBuildVersion(YarnVersionInfo.getBuildVersion());
    tsInfo.setTimelineServiceVersion(YarnVersionInfo.getVersion());
    tsInfo.setTimelineServiceVersionBuiltOn(YarnVersionInfo.getDate());
    return tsInfo;
  }

  public static InetSocketAddress getTimelineTokenServiceAddress(
      Configuration conf) {
    InetSocketAddress timelineServiceAddr = null;
    if (YarnConfiguration.useHttps(conf)) {
      timelineServiceAddr = conf.getSocketAddr(
          YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_PORT);
    } else {
      timelineServiceAddr = conf.getSocketAddr(
          YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_PORT);
    }
    return timelineServiceAddr;
  }

  public static Text buildTimelineTokenService(Configuration conf) {
    InetSocketAddress timelineServiceAddr =
        getTimelineTokenServiceAddress(conf);
    return SecurityUtil.buildTokenService(timelineServiceAddr);
  }

  public static String generateDefaultFlowName(String appName,
      ApplicationId appId) {
    return (appName != null &&
        !appName.equals(YarnConfiguration.DEFAULT_APPLICATION_NAME)) ?
        appName :
        "flow_" + appId.getClusterTimestamp() + "_" + appId.getId();
  }

  /**
   * Generate flow name tag.
   *
   * @param flowName flow name that identifies a distinct flow application which
   *                 can be run repeatedly over time
   * @return flow name tag.
   */
  public static String generateFlowNameTag(String flowName) {
    return FLOW_NAME_TAG_PREFIX + ":" + flowName;
  }

  /**
   * Generate flow version tag.
   *
   * @param flowVersion flow version that keeps track of the changes made to the
   *                    flow
   * @return flow version tag.
   */
  public static String generateFlowVersionTag(String flowVersion) {
    return FLOW_VERSION_TAG_PREFIX + ":" + flowVersion;
  }

  /**
   * Generate flow run ID tag.
   *
   * @param flowRunId flow run ID that identifies one instance (or specific
   *                  execution) of that flow
   * @return flow run id tag.
   */
  public static String generateFlowRunIdTag(long flowRunId) {
    return FLOW_RUN_ID_TAG_PREFIX + ":" + flowRunId;
  }
}
