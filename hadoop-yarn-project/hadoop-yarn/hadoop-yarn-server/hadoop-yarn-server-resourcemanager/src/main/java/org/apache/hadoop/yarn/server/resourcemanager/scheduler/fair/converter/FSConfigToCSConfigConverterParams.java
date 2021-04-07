/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

/**
 * POJO that holds values for the FS-&gt;CS converter.
 *
 */
public final class FSConfigToCSConfigConverterParams {
  private String yarnSiteXmlConfig;
  private String fairSchedulerXmlConfig;
  private String conversionRulesConfig;
  private boolean console;
  private String clusterResource;
  private String outputDirectory;
  private boolean convertPlacementRules;
  private boolean placementRulesToFile;
  private boolean usePercentages;
  private PreemptionMode preemptionMode;

  public enum PreemptionMode {
    ENABLED("enabled"),
    NO_POLICY("nopolicy"),
    OBSERVE_ONLY("observeonly");

    private String cliOption;

    PreemptionMode(String cliOption) {
      this.cliOption = cliOption;
    }

    public String getCliOption() {
      return cliOption;
    }

    public static PreemptionMode fromString(String cliOption) {
      if (cliOption != null && cliOption.trim().
          equals(PreemptionMode.OBSERVE_ONLY.getCliOption())) {
        return PreemptionMode.OBSERVE_ONLY;
      } else if (cliOption != null && cliOption.trim().
          equals(PreemptionMode.NO_POLICY.getCliOption())) {
        return PreemptionMode.NO_POLICY;
      } else {
        return PreemptionMode.ENABLED;
      }
    }
  }

  private FSConfigToCSConfigConverterParams() {
    //must use builder
  }

  public String getFairSchedulerXmlConfig() {
    return fairSchedulerXmlConfig;
  }

  public String getYarnSiteXmlConfig() {
    return yarnSiteXmlConfig;
  }

  public String getConversionRulesConfig() {
    return conversionRulesConfig;
  }

  public String getClusterResource() {
    return clusterResource;
  }

  public boolean isConsole() {
    return console;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public boolean isConvertPlacementRules() {
    return convertPlacementRules;
  }

  public boolean isPlacementRulesToFile() {
    return placementRulesToFile;
  }

  public boolean isUsePercentages() {
    return usePercentages;
  }

  public PreemptionMode getPreemptionMode() {
    return preemptionMode;
  }

  @Override
  public String toString() {
    return "FSConfigToCSConfigConverterParams{" +
        "yarnSiteXmlConfig='" + yarnSiteXmlConfig + '\'' +
        ", fairSchedulerXmlConfig='" + fairSchedulerXmlConfig + '\'' +
        ", conversionRulesConfig='" + conversionRulesConfig + '\'' +
        ", clusterResource='" + clusterResource + '\'' +
        ", console=" + console + '\'' +
        ", convertPlacementRules=" + convertPlacementRules +
        ", placementRulesToFile=" + placementRulesToFile +
        '}';
  }

  /**
   * Builder that can construct FSConfigToCSConfigConverterParams objects.
   *
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String yarnSiteXmlConfig;
    private String fairSchedulerXmlConfig;
    private String conversionRulesConfig;
    private boolean console;
    private String clusterResource;
    private String outputDirectory;
    private boolean convertPlacementRules;
    private boolean placementRulesToFile;
    private boolean usePercentages;
    private PreemptionMode preemptionMode;

    private Builder() {
    }

    public static Builder create() {
      return new Builder();
    }

    public Builder withYarnSiteXmlConfig(String config) {
      this.yarnSiteXmlConfig = config;
      return this;
    }

    public Builder withFairSchedulerXmlConfig(String config) {
      this.fairSchedulerXmlConfig = config;
      return this;
    }

    public Builder withConversionRulesConfig(String config) {
      this.conversionRulesConfig = config;
      return this;
    }

    public Builder withClusterResource(String res) {
      this.clusterResource = res;
      return this;
    }

    public Builder withConsole(boolean console) {
      this.console = console;
      return this;
    }

    public Builder withOutputDirectory(String outputDir) {
      this.outputDirectory = outputDir;
      return this;
    }

    public Builder withConvertPlacementRules(boolean convertPlacementRules) {
      this.convertPlacementRules = convertPlacementRules;
      return this;
    }

    public Builder withPlacementRulesToFile(boolean rulesToFile) {
      this.placementRulesToFile = rulesToFile;
      return this;
    }

    public Builder withUsePercentages(boolean usePercentages) {
      this.usePercentages = usePercentages;
      return this;
    }

    public Builder withDisablePreemption(PreemptionMode preemptionMode) {
      this.preemptionMode = preemptionMode;
      return this;
    }

    public FSConfigToCSConfigConverterParams build() {
      FSConfigToCSConfigConverterParams params =
          new FSConfigToCSConfigConverterParams();
      params.clusterResource = this.clusterResource;
      params.console = this.console;
      params.fairSchedulerXmlConfig = this.fairSchedulerXmlConfig;
      params.yarnSiteXmlConfig = this.yarnSiteXmlConfig;
      params.conversionRulesConfig = this.conversionRulesConfig;
      params.outputDirectory = this.outputDirectory;
      params.convertPlacementRules = this.convertPlacementRules;
      params.placementRulesToFile = this.placementRulesToFile;
      params.usePercentages = this.usePercentages;
      params.preemptionMode = this.preemptionMode;
      return params;
    }
  }
}
