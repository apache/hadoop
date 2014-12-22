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

package org.apache.hadoop.yarn.server.sharedcachemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.sharedcachemanager.SharedCacheManager;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.CleanerMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.ClientSCMMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.SharedCacheUploaderMetrics;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import com.google.inject.Inject;

/**
 * This class is to render the shared cache manager web ui overview page.
 */
@Private
@Unstable
public class SCMOverviewPage extends TwoColumnLayout {

  @Override protected void preHead(Page.HTML<_> html) {
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override protected Class<? extends SubView> content() {
    return SCMOverviewBlock.class;
  }

  @Override
  protected Class<? extends SubView> nav() {
    return SCMOverviewNavBlock.class;
  }

  static private class SCMOverviewNavBlock extends HtmlBlock {
    @Override
    protected void render(Block html) {
      html.div("#nav").h3("Tools").ul().li().a("/conf", "Configuration")._()
          .li().a("/stacks", "Thread dump")._().li().a("/logs", "Logs")._()
          .li().a("/metrics", "Metrics")._()._()._();
    }
  }

  static private class SCMOverviewBlock extends HtmlBlock {
    final SharedCacheManager scm;

    @Inject
    SCMOverviewBlock(SharedCacheManager scm, ViewContext ctx) {
      super(ctx);
      this.scm = scm;
    }

    @Override
    protected void render(Block html) {
      SCMMetricsInfo metricsInfo = new SCMMetricsInfo(
          CleanerMetrics.getInstance(), ClientSCMMetrics.getInstance(),
              SharedCacheUploaderMetrics.getInstance());
      info("Shared Cache Manager overview").
          _("Started on:", Times.format(scm.getStartTime())).
          _("Cache hits: ", metricsInfo.getCacheHits()).
          _("Cache misses: ", metricsInfo.getCacheMisses()).
          _("Cache releases: ", metricsInfo.getCacheReleases()).
          _("Accepted uploads: ", metricsInfo.getAcceptedUploads()).
          _("Rejected uploads: ", metricsInfo.getRejectUploads()).
          _("Deleted files by the cleaner: ", metricsInfo.getTotalDeletedFiles()).
          _("Processed files by the cleaner: ", metricsInfo.getTotalProcessedFiles());
      html._(InfoBlock.class);
    }
  }
}
