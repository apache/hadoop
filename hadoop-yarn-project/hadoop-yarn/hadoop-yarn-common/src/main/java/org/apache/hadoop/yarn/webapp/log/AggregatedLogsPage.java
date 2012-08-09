package org.apache.hadoop.yarn.webapp.log;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.ENTITY_STRING;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.THEMESWITCHER_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;


import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;


public class AggregatedLogsPage extends TwoColumnLayout {

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.server.nodemanager.webapp.NMView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override
  protected void preHead(Page.HTML<_> html) {
    String logEntity = $(ENTITY_STRING);
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = $(CONTAINER_ID);
    }
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = "UNKNOWN";
    }
    set(TITLE, join("Logs for ", logEntity));
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
    set(THEMESWITCHER_ID, "themeswitcher");
  }

  @Override
  protected Class<? extends SubView> content() {
    return AggregatedLogsBlock.class;
  }
  
  @Override
  protected Class<? extends SubView> nav() {
    return AggregatedLogsNavBlock.class;
  }
}
