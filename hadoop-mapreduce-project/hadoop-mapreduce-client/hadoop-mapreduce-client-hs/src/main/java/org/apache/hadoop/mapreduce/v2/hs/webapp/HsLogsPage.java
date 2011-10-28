package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.server.nodemanager.webapp.NMWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.server.nodemanager.webapp.NMWebParams.ENTITY_STRING;

import org.apache.hadoop.yarn.server.nodemanager.webapp.AggregatedLogsBlock;
import org.apache.hadoop.yarn.webapp.SubView;

public class HsLogsPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override protected void preHead(Page.HTML<_> html) {
    String logEntity = $(ENTITY_STRING);
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = $(CONTAINER_ID);
    }
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = "UNKNOWN";
    }
    commonPreHead(html);
  }

  /**
   * The content of this page is the JobBlock
   * @return HsJobBlock.class
   */
  @Override protected Class<? extends SubView> content() {
    return AggregatedLogsBlock.class;
  }
}