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

import static org.apache.commons.lang.StringEscapeUtils.escapeJavaScript;
import static org.apache.hadoop.yarn.util.StringHelper.djoin;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.split;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.HTML;

import com.google.common.collect.Lists;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class JQueryUI extends HtmlBlock {

  // UI params
  public static final String ACCORDION = "ui.accordion";
  public static final String ACCORDION_ID = ACCORDION +".id";
  public static final String DATATABLES = "ui.dataTables";
  public static final String DATATABLES_ID = DATATABLES +".id";
  public static final String DATATABLES_SELECTOR = DATATABLES +".selector";
  public static final String DIALOG = "ui.dialog";
  public static final String DIALOG_ID = DIALOG +".id";
  public static final String DIALOG_SELECTOR = DIALOG +".selector";
  public static final String PROGRESSBAR = "ui.progressbar";
  public static final String PROGRESSBAR_ID = PROGRESSBAR +".id";

  // common CSS classes
  public static final String _PROGRESSBAR =
      ".ui-progressbar.ui-widget.ui-widget-content.ui-corner-all";
  public static final String C_PROGRESSBAR =
      _PROGRESSBAR.replace('.', ' ').trim();
  public static final String _PROGRESSBAR_VALUE =
      ".ui-progressbar-value.ui-widget-header.ui-corner-left";
  public static final String C_PROGRESSBAR_VALUE =
      _PROGRESSBAR_VALUE.replace('.', ' ').trim();
  public static final String _INFO_WRAP =
      ".info-wrap.ui-widget-content.ui-corner-bottom";
  public static final String _TH = ".ui-state-default";
  public static final String C_TH = _TH.replace('.', ' ').trim();
  public static final String C_TABLE = "table";
  public static final String _INFO = ".info";
  public static final String _ODD = ".odd";
  public static final String _EVEN = ".even";

  @Override
  protected void render(Block html) {
    html.link(root_url("static/jquery/themes-1.9.1/base/jquery-ui.css"))
        .link(root_url("static/dt-1.9.4/css/jui-dt.css"))
        .script(root_url("static/jquery/jquery-1.8.2.min.js"))
        .script(root_url("static/jquery/jquery-ui-1.9.1.custom.min.js"))
        .script(root_url("static/dt-1.9.4/js/jquery.dataTables.min.js"))
        .script(root_url("static/yarn.dt.plugins.js"))
        .script(root_url("static/dt-sorting/natural.js"))
        .style("#jsnotice { padding: 0.2em; text-align: center; }",
            ".ui-progressbar { height: 1em; min-width: 5em }"); // required

    List<String> list = Lists.newArrayList();
    initAccordions(list);
    initDataTables(list);
    initDialogs(list);
    initProgressBars(list);

    if (!list.isEmpty()) {
      html.script().$type("text/javascript")._("$(function() {")
          ._(list.toArray())._("});")._();
    }
  }

  public static void jsnotice(HTML html) {
    html.
      div("#jsnotice.ui-state-error").
          _("This page will not function without javascript enabled."
            + " Please enable javascript on your browser.")._();
    html.
      script().$type("text/javascript").
        _("$('#jsnotice').hide();")._();
  }

  protected void initAccordions(List<String> list) {
    for (String id : split($(ACCORDION_ID))) {
      if (Html.isValidId(id)) {
        String init = $(initID(ACCORDION, id));
        if (init.isEmpty()) {
          init = "{autoHeight: false}";
        }
        list.add(join("  $('#", id, "').accordion(", init, ");"));
      }
    }
  }

  protected void initDataTables(List<String> list) {
    String defaultInit = "{bJQueryUI: true, sPaginationType: 'full_numbers'}";
    String stateSaveInit = "bStateSave : true, " +
        "\"fnStateSave\": function (oSettings, oData) { " +
              " data = oData.aoSearchCols;"
              + "for(i =0 ; i < data.length; i ++) {"
              + "data[i].sSearch = \"\""
              + "}"
        + " sessionStorage.setItem( oSettings.sTableId, JSON.stringify(oData) ); }, " +
          "\"fnStateLoad\": function (oSettings) { " +
              "return JSON.parse( sessionStorage.getItem(oSettings.sTableId) );}, ";
      
    for (String id : split($(DATATABLES_ID))) {
      if (Html.isValidId(id)) {
        String init = $(initID(DATATABLES, id));
        if (init.isEmpty()) {
          init = defaultInit;
        }
        // for inserting stateSaveInit
        int pos = init.indexOf('{') + 1;  
        init = new StringBuffer(init).insert(pos, stateSaveInit).toString(); 
        list.add(join(id,"DataTable =  $('#", id, "').dataTable(", init,
                      ").fnSetFilteringDelay(188);"));
        String postInit = $(postInitID(DATATABLES, id));
        if(!postInit.isEmpty()) {
          list.add(postInit);
        }
      }
    }
    String selector = $(DATATABLES_SELECTOR);
    if (!selector.isEmpty()) {
      String init = $(initSelector(DATATABLES));
      if (init.isEmpty()) {
        init = defaultInit;
      }      
      int pos = init.indexOf('{') + 1;  
      init = new StringBuffer(init).insert(pos, stateSaveInit).toString();  
      list.add(join("  $('", escapeJavaScript(selector), "').dataTable(", init,
               ").fnSetFilteringDelay(288);"));      
      
    }
  }

  protected void initDialogs(List<String> list) {
    String defaultInit = "{autoOpen: false, show: transfer, hide: explode}";
    for (String id : split($(DIALOG_ID))) {
      if (Html.isValidId(id)) {
        String init = $(initID(DIALOG, id));
        if (init.isEmpty()) {
          init = defaultInit;
        }
        String opener = $(djoin(DIALOG, id, "opener"));
        list.add(join("  $('#", id, "').dialog(", init, ");"));
        if (!opener.isEmpty() && Html.isValidId(opener)) {
          list.add(join("  $('#", opener, "').click(function() { ",
                   "$('#", id, "').dialog('open'); return false; });"));
        }
      }
    }
    String selector = $(DIALOG_SELECTOR);
    if (!selector.isEmpty()) {
      String init = $(initSelector(DIALOG));
      if (init.isEmpty()) {
        init = defaultInit;
      }
      list.add(join("  $('", escapeJavaScript(selector),
               "').click(function() { $(this).children('.dialog').dialog(",
               init, "); return false; });"));
    }
  }

  protected void initProgressBars(List<String> list) {
    for (String id : split($(PROGRESSBAR_ID))) {
      if (Html.isValidId(id)) {
        String init = $(initID(PROGRESSBAR, id));
        list.add(join("  $('#", id, "').progressbar(", init, ");"));
      }
    }
  }

  public static String initID(String name, String id) {
    return djoin(name, id, "init");
  }
  
  public static String postInitID(String name, String id) {
    return djoin(name, id, "postinit");
  }

  public static String initSelector(String name) {
    return djoin(name, "selector.init");
  }

  public static StringBuilder tableInit() {
    return new StringBuilder("{bJQueryUI:true, ").
        append("sPaginationType: 'full_numbers', iDisplayLength:20, ").
        append("aLengthMenu:[20, 40, 60, 80, 100]");
  }
}
