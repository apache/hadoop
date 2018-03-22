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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import com.google.inject.Inject;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

public class AboutBlock extends HtmlBlock {
  @Inject
  AboutBlock(View.ViewContext ctx) {
    super(ctx);
  }

  @Override
  protected void render(Block html) {
    TimelineAbout tsInfo = TimelineUtils.createTimelineAbout(
        "Timeline Server - Generic History Service UI");
    info("Timeline Server Overview").
        __("Timeline Server Version:", tsInfo.getTimelineServiceBuildVersion() +
            " on " + tsInfo.getTimelineServiceVersionBuiltOn()).
        __("Hadoop Version:", tsInfo.getHadoopBuildVersion() +
            " on " + tsInfo.getHadoopVersionBuiltOn());
    html.__(InfoBlock.class);
  }
}
