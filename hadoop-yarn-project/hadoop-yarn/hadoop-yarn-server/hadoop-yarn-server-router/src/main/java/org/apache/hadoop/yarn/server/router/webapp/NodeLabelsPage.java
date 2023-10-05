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

package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import static org.apache.hadoop.yarn.server.router.webapp.RouterWebServiceUtil.generateWebTitle;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_SC;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_LABEL;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;

/**
 * Renders a block for the nodelabels with metrics information.
 */
public class NodeLabelsPage extends RouterView {

  @Override
  protected void preHead(Hamlet.HTML<__> html) {
    commonPreHead(html);
    String type = $(NODE_SC);
    String nodeLabel = $(NODE_LABEL);
    String title = "Node labels of the cluster";

    if (nodeLabel != null && !nodeLabel.isEmpty()) {
      title = generateWebTitle(title, nodeLabel);
    } else if (type != null && !type.isEmpty()) {
      title = generateWebTitle(title, type);
    }

    setTitle(title);
    set(DATATABLES_ID, "nodelabels");
    setTableStyles(html, "nodelabels", ".healthStatus {width:10em}", ".healthReport {width:10em}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return NodeLabelsBlock.class;
  }
}
