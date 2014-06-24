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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import java.util.Date;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

import com.google.inject.Inject;

public class NodePage extends NMView {

  private static final long BYTES_IN_MB = 1024 * 1024;

  @Override
  protected void commonPreHead(HTML<_> html) {
    super.commonPreHead(html);

    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return NodeBlock.class;
  }

  public static class NodeBlock extends HtmlBlock {

    private final Context context;
    private final ResourceView resourceView;

    @Inject
    public NodeBlock(Context context, ResourceView resourceView) {
      this.context = context;
      this.resourceView = resourceView;
    }

    @Override
    protected void render(Block html) {
      NodeInfo info = new NodeInfo(this.context, this.resourceView);
      info("NodeManager information")
          ._("Total Vmem allocated for Containers",
              StringUtils.byteDesc(info.getTotalVmemAllocated() * BYTES_IN_MB))
          ._("Vmem enforcement enabled",
              info.isVmemCheckEnabled())
          ._("Total Pmem allocated for Container",
              StringUtils.byteDesc(info.getTotalPmemAllocated() * BYTES_IN_MB))
          ._("Pmem enforcement enabled",
              info.isVmemCheckEnabled())
           ._("Total VCores allocated for Containers",
              String.valueOf(info.getTotalVCoresAllocated())) 
          ._("NodeHealthyStatus",
              info.getHealthStatus())
          ._("LastNodeHealthTime", new Date(
              info.getLastNodeUpdateTime()))
          ._("NodeHealthReport",
              info.getHealthReport())
          ._("Node Manager Version:", info.getNMBuildVersion() +
              " on " + info.getNMVersionBuiltOn())
          ._("Hadoop Version:", info.getHadoopBuildVersion() +
              " on " + info.getHadoopVersionBuiltOn());
      html._(InfoBlock.class);
    }
  }
}
