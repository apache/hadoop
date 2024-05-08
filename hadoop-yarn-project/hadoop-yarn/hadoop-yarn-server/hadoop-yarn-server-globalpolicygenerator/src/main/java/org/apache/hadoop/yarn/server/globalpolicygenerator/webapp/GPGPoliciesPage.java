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

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

/**
 * Overview page for the GPG Policies Web UI.
 */
public class GPGPoliciesPage extends TwoColumnLayout {

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
  }

  protected void commonPreHead(Page.HTML<__> html) {
    setTitle("Global Policy Generator Policies");
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
    set(DATATABLES_ID, "policies");
  }

  @Override
  protected Class<? extends SubView> content() {
    return GPGPoliciesBlock.class;
  }

  @Override
  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }

}
