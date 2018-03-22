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

import org.apache.hadoop.yarn.server.webapp.ErrorsAndWarningsBlock;
import org.apache.hadoop.yarn.webapp.SubView;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * Class to display the Errors and Warnings page for the AHS.
 */
public class AHSErrorsAndWarningsPage extends AHSView {

  @Override
  protected Class<? extends SubView> content() {
    return ErrorsAndWarningsBlock.class;
  }

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    String title = "Errors and Warnings in the Application History Server";
    setTitle(title);
    String tableId = "messages";
    set(DATATABLES_ID, tableId);
    set(initID(DATATABLES, tableId), tablesInit());
    setTableStyles(html, tableId, ".message {width:50em}",
        ".count {width:8em}", ".lasttime {width:16em}");
  }

  private String tablesInit() {
    StringBuilder b = tableInit().append(", aoColumnDefs: [");
    b.append("{'sType': 'string', 'aTargets': [ 0 ]}");
    b.append(", {'sType': 'string', 'bSearchable': true, 'aTargets': [ 1 ]}");
    b.append(", {'sType': 'numeric', 'bSearchable': false, 'aTargets': [ 2 ]}");
    b.append(", {'sType': 'date', 'aTargets': [ 3 ] }]");
    b.append(", aaSorting: [[3, 'desc']]}");
    return b.toString();
  }
}
