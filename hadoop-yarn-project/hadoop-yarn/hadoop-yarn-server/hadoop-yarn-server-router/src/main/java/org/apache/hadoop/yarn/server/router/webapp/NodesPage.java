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

import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.webapp.SubView;

class NodesPage extends RouterView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    String type = $(NODE_STATE);
    String title = "Nodes of the cluster";
    if (type != null && !type.isEmpty()) {
      title = title + " (" + type + ")";
    }
    setTitle(title);
    set(DATATABLES_ID, "nodes");
    set(initID(DATATABLES, "nodes"), nodesTableInit());
    setTableStyles(html, "nodes", ".healthStatus {width:10em}",
        ".healthReport {width:10em}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return NodesBlock.class;
  }

  private String nodesTableInit() {
    StringBuilder b = tableInit().append(", aoColumnDefs: [");
    b.append("{'bSearchable': false, 'aTargets': [ 7 ]}");
    b.append(", {'sType': 'title-numeric', 'bSearchable': false, "
        + "'aTargets': [ 2, 3, 4, 5, 6 ] }");
    b.append(", {'sType': 'title-numeric', 'aTargets': [ 5 ]}");
    b.append("]}");
    return b.toString();
  }
}