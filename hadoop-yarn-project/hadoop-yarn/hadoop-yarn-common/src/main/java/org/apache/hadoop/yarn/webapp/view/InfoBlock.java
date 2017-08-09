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

package org.apache.hadoop.yarn.webapp.view;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.C_TH;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TD;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;

import com.google.inject.Inject;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class InfoBlock extends HtmlBlock {
  final ResponseInfo info;

  @Inject InfoBlock(ResponseInfo info) {
    this.info = info;
  }

  @Override protected void render(Block html) {
    TABLE<DIV<Hamlet>> table = html.
      div(_INFO_WRAP).
        table(_INFO).
          tr().
            th().$class(C_TH).$colspan(2).__(info.about()).__().__();
    int i = 0;
    for (ResponseInfo.Item item : info) {
      TR<TABLE<DIV<Hamlet>>> tr = table.
        tr((++i % 2 != 0) ? _ODD : _EVEN).
          th(item.key);
      String value = String.valueOf(item.value);
      if (item.url == null) {
        if (!item.isRaw) {
          TD<TR<TABLE<DIV<Hamlet>>>> td = tr.td();
          if ( value.lastIndexOf('\n') > 0) {
            String []lines = value.split("\n");
        	DIV<TD<TR<TABLE<DIV<Hamlet>>>>> singleLineDiv;
            for ( String line :lines) {
              singleLineDiv = td.div();
              singleLineDiv.__(line);
              singleLineDiv.__();
            }
          } else {
            td.__(value);
          }
          td.__();
        } else {
          tr.td()._r(value).__();
        }
      } else {
        tr.
          td().
            a(url(item.url), value).__();
      }
      tr.__();
    }
    table.__().__();
  }
}
