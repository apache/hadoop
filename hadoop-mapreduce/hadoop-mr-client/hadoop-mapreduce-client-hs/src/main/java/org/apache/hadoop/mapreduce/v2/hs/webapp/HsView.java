package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.hadoop.mapreduce.v2.app.webapp.JobsBlock;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

public class HsView extends TwoColumnLayout {
  @Override protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    set(DATATABLES_ID, "jobs");
    set(initID(DATATABLES, "jobs"), jobsTableInit());
    setTableStyles(html, "jobs");
  }

  protected void commonPreHead(Page.HTML<_> html) {
    //html.meta_http("refresh", "10");
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
    set(THEMESWITCHER_ID, "themeswitcher");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#nav()
   */

  @Override
  protected Class<? extends SubView> nav() {
    return org.apache.hadoop.mapreduce.v2.app.webapp.NavBlock.class;
  }

  @Override
  protected Class<? extends SubView> content() {
    return JobsBlock.class;
  }

  private String jobsTableInit() {
    return tableInit().
        append(",aoColumns:[{sType:'title-numeric'},").
        append("null,null,{sType:'title-numeric', bSearchable:false},null,").
        append("null,{sType:'title-numeric',bSearchable:false}, null, null]}").
        toString();
  }
}
