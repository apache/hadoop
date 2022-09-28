package org.apache.hadoop.yarn.server.router.webapp;

import com.sun.jersey.api.client.Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public abstract class RouterBlock extends HtmlBlock {

  private final Router router;

  public RouterBlock(Router router, ViewContext ctx) {
    super(ctx);
    this.router = router;
  }

  /**
   * Initialize User Help Information Div.
   * When the user does not configure the Yarn Federation function, prompt the user.
   *
   * @param html html page
   * @param isEnabled true, Federation is enabled; false, Federation is not enabled
   */
  protected void initUserHelpInformationDiv(HtmlBlock.Block html, boolean isEnabled) {
    if (!isEnabled) {
      html.style(".alert {padding: 15px; margin-bottom: 20px; " +
          " border: 1px solid transparent; border-radius: 4px;}");
      html.style(".alert-dismissable {padding-right: 35px;}");
      html.style(".alert-info {color: #856404;background-color: #fff3cd;border-color: #ffeeba;}");

      Hamlet.DIV<Hamlet> div = html.div("#div_id").$class("alert alert-dismissable alert-info");
      div.p().$style("color:red").__("Federation is not Enabled.").__()
          .p().__()
          .p().__("We can refer to the following documents to configure Yarn Federation. ").__()
          .p().__()
          .a("https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/Federation.html",
          "Hadoop: YARN Federation").
          __();
    }
  }

  /**
   * Get RouterClusterMetrics Info.
   *
   * @return Router ClusterMetricsInfo.
   */
  protected ClusterMetricsInfo getRouterClusterMetricsInfo() {
    Configuration conf = this.router.getConfig();
    String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);
    Client client = RouterWebServiceUtil.createJerseyClient(conf);
    ClusterMetricsInfo metrics = RouterWebServiceUtil
        .genericForward(webAppAddress, null, ClusterMetricsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
        conf, client);

    return metrics;
  }
}
