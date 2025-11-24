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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * Overview block for the GPG Web UI.
 */
public class GPGOverviewBlock extends HtmlBlock {

  private GlobalPolicyGenerator globalPolicyGenerator;

  @Inject
  GPGOverviewBlock(GlobalPolicyGenerator gpg, ViewContext ctx) {
    super(ctx);
    this.globalPolicyGenerator = gpg;
  }

  @Override
  protected void render(Block html) {
    Configuration config = this.globalPolicyGenerator.getConfig();

    String appCleaner = "disable";
    long appCleanerIntervalMs = config.getTimeDuration(YarnConfiguration.GPG_APPCLEANER_INTERVAL_MS,
        YarnConfiguration.DEFAULT_GPG_APPCLEANER_INTERVAL_MS, TimeUnit.MILLISECONDS);
    if (appCleanerIntervalMs > 0) {
      appCleaner = "enable, interval : " + appCleanerIntervalMs + " ms";
    }

    String scCleaner = "disable";
    long scCleanerIntervalMs = config.getTimeDuration(
        YarnConfiguration.GPG_SUBCLUSTER_CLEANER_INTERVAL_MS,
        YarnConfiguration.DEFAULT_GPG_SUBCLUSTER_CLEANER_INTERVAL_MS, TimeUnit.MILLISECONDS);
    if (scCleanerIntervalMs > 0) {
      scCleaner = "enable, interval : " + scCleanerIntervalMs + " ms";
    }

    String pgGenerator = "disable";
    long policyGeneratorIntervalMillis = config.getTimeDuration(
        YarnConfiguration.GPG_POLICY_GENERATOR_INTERVAL,
        YarnConfiguration.DEFAULT_GPG_POLICY_GENERATOR_INTERVAL, TimeUnit.MILLISECONDS);

    if (policyGeneratorIntervalMillis > 0) {
      pgGenerator = "enable, interval : " + policyGeneratorIntervalMillis + " ms";
    }

    String policy = config.get(YarnConfiguration.GPG_GLOBAL_POLICY_CLASS,
        YarnConfiguration.DEFAULT_GPG_GLOBAL_POLICY_CLASS);

    info("GPG Details")
        .__("GPG started on", new Date(GlobalPolicyGenerator.getGPGStartupTime()))
        .__("GPG application cleaner", appCleaner)
        .__("GPG subcluster cleaner", scCleaner)
        .__("GPG policy generator", pgGenerator)
        .__("GPG policy generator class", policy)
        .__("GPG Version", YarnVersionInfo.getVersion())
        .__("Hadoop Version", VersionInfo.getVersion());

    html.__(InfoBlock.class);
  }
}
