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

package org.apache.hadoop.yarn.server.nodemanager.health;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.NodeHealthDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.util.resource.ResourceUtils.addResourcesFileToConf;

/**
 * This class provides the functionality of reading a score of the node from
 * a xml file. The file is read only when the lastModified time is changed.
 * This is the default implementation of {@link NodeHealthDetailsReporter}
 * See {@link NodeHealthDetailsReporter}
 */
public class FileBasedNodeHealthDetails extends NodeHealthDetailsReporter
    implements HealthReporter {

  public static final Logger LOG =
      LoggerFactory.getLogger(FileBasedNodeHealthDetails.class);

  private NodeHealthDetails nodeHealthDetails;
  private long lastModified = -1;
  private File resourceScoreFile;
  private String nodeHealthScoreFilename;

  private boolean isHealthy;
  private String healthReport;



  public FileBasedNodeHealthDetails(Configuration conf) {
    this.nodeHealthDetails = NodeHealthDetails.newInstance(0);
    nodeHealthScoreFilename = conf.get(
        YarnConfiguration.NM_HEALTH_CHECK_SCORE_FILE);
    resourceScoreFile = new File(nodeHealthScoreFilename);
    this.isHealthy = true;
    this.healthReport = "";
  }

  /**
   * Reads the score of each resource and gives out a summation of overall
   * score of the node from a xml file. The file is read only if the last
   * modified value changes. Node Health Details needs to be enabled.
   * Otherwise, the score is defaulted to 0.
   */
  @Override
  public void updateNodeHealthDetails() {
    try {
      if (resourceScoreFile.lastModified() <= lastModified) {
        LOG.debug("resourceScoreFile [{}] has not been modified " + "from last check",
            resourceScoreFile);
      } else {
        HashMap<String, Integer> resourcesScore = new HashMap<>();
        readXMLFile(nodeHealthScoreFilename, resourcesScore);

        Integer overallScore =
            resourcesScore.values().stream().reduce(0, Integer::sum);
        nodeHealthDetails =
            NodeHealthDetails.newInstance(overallScore, resourcesScore);

        lastModified = resourceScoreFile.lastModified();
        setNodeAsHealthy();
      }
    } catch (Exception e) {
      LOG.error("Error reading Node Health Checker score file", e);
      setNodeAsUnhealthy(e);
    }

  }

  /**
   * Reads the input xml file. The tags of the file leverages {
   * @link Configuration} tags.
   * @param filename The path of the file.
   * @param map The resources map of the node. This will hold the score of each
   *            property.
   */
  public void readXMLFile(String filename, Map<String, Integer> map) {
    Configuration conf = new Configuration(false);
    addResourcesFileToConf(filename, conf);
    for (Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      String score = entry.getValue();
      Integer value = (score == null) ? null : Integer.parseInt(score);
      map.put(key, value);
    }

  }

  public void setNodeAsHealthy() {
    this.isHealthy = true;
    this.healthReport = "";
  }

  public void setNodeAsUnhealthy(Exception e) {
    this.isHealthy = false;
    this.healthReport = StringUtils.stringifyException(e);
  }

  @Override
  public NodeHealthDetails getNodeHealthDetails() {
    return nodeHealthDetails;
  }

  @Override
  public boolean isHealthy() {
    return this.isHealthy;
  }

  @Override
  public String getHealthReport() {
    return this.healthReport;
  }

  @Override
  public long getLastHealthReportTime() {
    return this.lastModified;
  }
}
