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

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.util.YarnVersionInfo;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

/**
 * Overview block for the GPG Web UI.
 */
public class GPGOverviewBlock extends HtmlBlock {

  @Inject
  GPGOverviewBlock(GlobalPolicyGenerator gpg, ViewContext ctx) {
    super(ctx);
  }

  @Override
  protected void render(Block html) {
    info("GPG Details")
        .__("GPG started on", new Date(GlobalPolicyGenerator.getGPGStartupTime()))
        .__("GPG Version", YarnVersionInfo.getVersion())
        .__("Hadoop Version", VersionInfo.getVersion());

    html.__(InfoBlock.class);
  }
}
