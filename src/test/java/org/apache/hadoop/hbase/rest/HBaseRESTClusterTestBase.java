package org.apache.hadoop.hbase.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.sun.jersey.spi.container.servlet.ServletContainer;

public class HBaseRESTClusterTestBase extends HBaseClusterTestCase 
    implements Constants {

  static final Log LOG =
    LogFactory.getLog(HBaseRESTClusterTestBase.class);

  protected int testServletPort;
  Server server;

  protected void setUp() throws Exception {
    super.setUp();
    startServletContainer();
  }

  protected void tearDown() throws Exception {
    stopServletContainer();
    super.tearDown();
  }

  private void startServletContainer() throws Exception {
    if (server != null) {
      LOG.error("ServletContainer already running");
      return;
    }

    // set up the Jersey servlet container for Jetty
    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter(
      "com.sun.jersey.config.property.resourceConfigClass",
      ResourceConfig.class.getCanonicalName());
    sh.setInitParameter("com.sun.jersey.config.property.packages",
      "jetty");

    LOG.info("configured " + ServletContainer.class.getName());
    
    // set up Jetty and run the embedded server
    server = new Server(0);
    server.setSendServerVersion(false);
    server.setSendDateHeader(false);
      // set up context
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addServlet(sh, "/*");
      // start the server
    server.start();
      // get the port
    testServletPort = server.getConnectors()[0].getLocalPort();
    
    LOG.info("started " + server.getClass().getName() + " on port " + 
      testServletPort);
  }

  private void stopServletContainer() {
    if (server != null) try {
      server.stop();
      server = null;
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }
}
