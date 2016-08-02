/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.rest.agent;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.WebApplication;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.sun.jersey.spi.container.servlet.WebConfig;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.providers.agent.AgentKeys;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.services.security.SecurityUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.security.SslSelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ext.Provider;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.util.Set;

/**
 *
 */
public class AgentWebApp implements Closeable {
  protected static final Logger LOG = LoggerFactory.getLogger(AgentWebApp.class);
  private int port;
  private int securedPort;
  private static Server agentServer;
  public static final String BASE_PATH = "slideragent";

  public static class Builder {
    final String name;
    final String wsName;
    final WebAppApi application;
    int port;
    int securedPort;
    MapOperations configsMap;

    public Builder(String name, String wsName, WebAppApi application) {
      this.name = name;
      this.wsName = wsName;
      this.application = application;
    }

    public Builder withComponentConfig(MapOperations appMasterConfig) {
      this.configsMap = appMasterConfig;
      return this;
    }

    public Builder withPort (int port) {
      this.port = port;
      return this;
    }

    public Builder withSecuredPort (int securedPort) {
      this.securedPort = securedPort;
      return this;
    }

    public AgentWebApp start() throws IOException {
      if (configsMap == null) {
        throw new IllegalStateException("No SSL Configuration Available");
      }

      agentServer = new Server();
      agentServer.setThreadPool(
          new QueuedThreadPool(
              configsMap.getOptionInt("agent.threadpool.size.max", 25)));
      agentServer.setStopAtShutdown(true);
      agentServer.setGracefulShutdown(1000);

      SslSelectChannelConnector ssl1WayConnector = createSSLConnector(false, port);
      SslSelectChannelConnector ssl2WayConnector =
          createSSLConnector(Boolean.valueOf(
              configsMap.getOption(AgentKeys.KEY_AGENT_TWO_WAY_SSL_ENABLED,
                                   "false")), securedPort);
      agentServer.setConnectors(new Connector[]{ssl1WayConnector,
          ssl2WayConnector});

      ServletHolder agent = new ServletHolder(new AgentServletContainer());
      Context agentRoot = new Context(agentServer, "/", Context.SESSIONS);

      agent.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
                             "com.sun.jersey.api.core.PackagesResourceConfig");
      agent.setInitParameter("com.sun.jersey.config.property.packages",
                             "org.apache.slider.server.appmaster.web.rest.agent");
      agent.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature",
                             "true");
//      agent.setInitParameter("com.sun.jersey.spi.container.ContainerRequestFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
//      agent.setInitParameter("com.sun.jersey.spi.container.ContainerResponseFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
//      agent.setInitParameter("com.sun.jersey.config.feature.Trace", "true");
      agentRoot.addServlet(agent, "/*");

      try {
        openListeners();
        agentServer.start();
      } catch (IOException e) {
        LOG.error("Unable to start agent server", e);
        throw e;
      } catch (Exception e) {
        LOG.error("Unable to start agent server", e);
        throw new IOException("Unable to start agent server: " + e, e);
      }

      AgentWebApp webApp = new AgentWebApp();
      webApp.setPort(getConnectorPort(agentServer, 0));
      webApp.setSecuredPort(getConnectorPort(agentServer, 1));
      return webApp;

    }

    private void openListeners() throws Exception {
      // from HttpServer2.openListeners()
      for (Connector listener : agentServer.getConnectors()) {
        if (listener.getLocalPort() != -1) {
          // This listener is either started externally or has been bound
          continue;
        }
        int port = listener.getPort();
        while (true) {
          // jetty has a bug where you can't reopen a listener that previously
          // failed to open w/o issuing a close first, even if the port is changed
          try {
            listener.close();
            listener.open();
            LOG.info("Jetty bound to port " + listener.getLocalPort());
            break;
          } catch (BindException ex) {
            if (port == 0) {
              BindException be = new BindException("Port in use: "
                  + listener.getHost() + ":" + listener.getPort());
              be.initCause(ex);
              throw be;
            }
          }
          // try the next port number
          listener.setPort(++port);
          Thread.sleep(100);
        }
      }
    }

    private SslSelectChannelConnector createSSLConnector(boolean needClientAuth, int port) {
      SslSelectChannelConnector sslConnector = new
          SslSelectChannelConnector();

      String keystore = SecurityUtils.getSecurityDir() +
                        File.separator + "keystore.p12";
      String srvrCrtPass = SecurityUtils.getKeystorePass();
      sslConnector.setKeystore(keystore);
      sslConnector.setTruststore(keystore);
      sslConnector.setPassword(srvrCrtPass);
      sslConnector.setKeyPassword(srvrCrtPass);
      sslConnector.setTrustPassword(srvrCrtPass);
      sslConnector.setKeystoreType("PKCS12");
      sslConnector.setTruststoreType("PKCS12");
      sslConnector.setNeedClientAuth(needClientAuth);

      sslConnector.setPort(port);
      sslConnector.setAcceptors(2);
      return sslConnector;
    }

    @Provider
    public class WebAppApiProvider extends
        SingletonTypeInjectableProvider<javax.ws.rs.core.Context, WebAppApi> {

      public WebAppApiProvider () {
        super(WebAppApi.class, application);
      }
    }

    public class AgentServletContainer extends ServletContainer {
      public AgentServletContainer() {
        super();
      }

      @Override
      protected void configure(WebConfig wc,
                               ResourceConfig rc,
                               WebApplication wa) {
        super.configure(wc, rc, wa);
        Set<Object> singletons = rc.getSingletons();
        singletons.add(new WebAppApiProvider());
      }
    }

    private int getConnectorPort(Server webServer, int index) {
      Preconditions.checkArgument(index >= 0);
      if (index > webServer.getConnectors().length)
        throw new IllegalStateException("Illegal connect index requested");

      Connector c = webServer.getConnectors()[index];
      if (c.getLocalPort() == -1) {
        // The connector is not bounded
        throw new IllegalStateException("The connector is not bound to a port");
      }

      return c.getLocalPort();
    }
  }

  public static Builder $for(String name, WebAppApi app, String wsPrefix) {
    return new Builder(name, wsPrefix, app);
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setSecuredPort(int securedPort) {
    this.securedPort = securedPort;
  }

  public int getSecuredPort() {
    return securedPort;
  }

  public void close() throws IOException{
    //need to stop server and reset injector
    try {
      agentServer.stop();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e.toString(), e);
    }
  }

}
