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

package org.apache.hadoop.yarn.webapp;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServlet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;
import org.apache.hadoop.security.http.XFrameOptionsFilter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;

/**
 * Helpers to create an embedded webapp.
 *
 * <b>Quick start:</b>
 * <pre>
 *   WebApp wa = WebApps.$for(myApp).start();</pre>
 * Starts a webapp with default routes binds to 0.0.0.0 (all network interfaces)
 * on an ephemeral port, which can be obtained with:<pre>
 *   int port = wa.port();</pre>
 * <b>With more options:</b>
 * <pre>
 *   WebApp wa = WebApps.$for(myApp).at(address, port).
 *                        with(configuration).
 *                        start(new WebApp() {
 *     &#064;Override public void setup() {
 *       route("/foo/action", FooController.class);
 *       route("/foo/:id", FooController.class, "show");
 *     }
 *   });</pre>
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class WebApps {
  static final Logger LOG = LoggerFactory.getLogger(WebApps.class);
  public static class Builder<T> {

    static class ServletStruct {
      public Class<? extends HttpServlet> clazz;
      public String name;
      public String spec;
    }
    
    final String name;
    final String wsName;
    final Class<T> api;
    final T application;
    String bindAddress = "0.0.0.0";
    int port = 0;
    boolean findPort = false;
    Configuration conf;
    Policy httpPolicy = null;
    boolean devMode = false;
    private String spnegoPrincipalKey;
    private String spnegoKeytabKey;
    private String csrfConfigPrefix;
    private String xfsConfigPrefix;
    private final HashSet<ServletStruct> servlets = new HashSet<ServletStruct>();
    private final HashMap<String, Object> attributes = new HashMap<String, Object>();

    Builder(String name, Class<T> api, T application, String wsName) {
      this.name = name;
      this.api = api;
      this.application = application;
      this.wsName = wsName;
    }

    Builder(String name, Class<T> api, T application) {
      this(name, api, application, null);
    }

    public Builder<T> at(String bindAddress) {
      String[] parts = StringUtils.split(bindAddress, ':');
      if (parts.length == 2) {
        int port = Integer.parseInt(parts[1]);
        return at(parts[0], port, port == 0);
      }
      return at(bindAddress, 0, true);
    }

    public Builder<T> at(int port) {
      return at("0.0.0.0", port, port == 0);
    }

    public Builder<T> at(String address, int port, boolean findPort) {
      this.bindAddress = checkNotNull(address, "bind address");
      this.port = port;
      this.findPort = findPort;
      return this;
    }

    public Builder<T> withAttribute(String key, Object value) {
      attributes.put(key, value);
      return this;
    }
    
    public Builder<T> withServlet(String name, String pathSpec, 
        Class<? extends HttpServlet> servlet) {
      ServletStruct struct = new ServletStruct();
      struct.clazz = servlet;
      struct.name = name;
      struct.spec = pathSpec;
      servlets.add(struct);
      return this;
    }
    
    public Builder<T> with(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder<T> withHttpPolicy(Configuration conf, Policy httpPolicy) {
      this.conf = conf;
      this.httpPolicy = httpPolicy;
      return this;
    }

    public Builder<T> withHttpSpnegoPrincipalKey(String spnegoPrincipalKey) {
      this.spnegoPrincipalKey = spnegoPrincipalKey;
      return this;
    }
    
    public Builder<T> withHttpSpnegoKeytabKey(String spnegoKeytabKey) {
      this.spnegoKeytabKey = spnegoKeytabKey;
      return this;
    }

    /**
     * Enable the CSRF filter.
     * @param prefix The config prefix that identifies the
     *                         CSRF parameters applicable for this filter
     *                         instance.
     * @return the Builder instance
     */
    public Builder<T> withCSRFProtection(String prefix) {
      this.csrfConfigPrefix = prefix;
      return this;
    }

    /**
     * Enable the XFS filter.
     * @param prefix The config prefix that identifies the
     *                         XFS parameters applicable for this filter
     *                         instance.
     * @return the Builder instance
     */
    public Builder<T> withXFSProtection(String prefix) {
      this.xfsConfigPrefix = prefix;
      return this;
    }

    public Builder<T> inDevMode() {
      devMode = true;
      return this;
    }

    public WebApp build(WebApp webapp) {
      if (webapp == null) {
        webapp = new WebApp() {
          @Override
          public void setup() {
            // Defaults should be fine in usual cases
          }
        };
      }
      webapp.setName(name);
      webapp.setWebServices(wsName);
      String basePath = "/" + name;
      webapp.setRedirectPath(basePath);
      List<String> pathList = new ArrayList<String>();
      if (basePath.equals("/")) { 
        webapp.addServePathSpec("/*");
        pathList.add("/*");
      }  else {
        webapp.addServePathSpec(basePath);
        webapp.addServePathSpec(basePath + "/*");
        pathList.add(basePath + "/*");
      }
      if (wsName != null && !wsName.equals(basePath)) {
        if (wsName.equals("/")) { 
          webapp.addServePathSpec("/*");
          pathList.add("/*");
        } else {
          webapp.addServePathSpec("/" + wsName);
          webapp.addServePathSpec("/" + wsName + "/*");
          pathList.add("/" + wsName + "/*");
        }
      }
      if (conf == null) {
        conf = new Configuration();
      }
      try {
        if (application != null) {
          webapp.setHostClass(application.getClass());
        } else {
          String cls = inferHostClass();
          LOG.debug("setting webapp host class to {}", cls);
          webapp.setHostClass(Class.forName(cls));
        }
        if (devMode) {
          if (port > 0) {
            try {
              new URL("http://localhost:"+ port +"/__stop").getContent();
              LOG.info("stopping existing webapp instance");
              Thread.sleep(100);
            } catch (ConnectException e) {
              LOG.info("no existing webapp instance found: {}", e.toString());
            } catch (Exception e) {
              // should not be fatal
              LOG.warn("error stopping existing instance: {}", e.toString());
            }
          } else {
            LOG.error("dev mode does NOT work with ephemeral port!");
            System.exit(1);
          }
        }
        String httpScheme;
        if (this.httpPolicy == null) {
          httpScheme = WebAppUtils.getHttpSchemePrefix(conf);
        } else {
          httpScheme =
              (httpPolicy == Policy.HTTPS_ONLY) ? WebAppUtils.HTTPS_PREFIX
                  : WebAppUtils.HTTP_PREFIX;
        }
        HttpServer2.Builder builder = new HttpServer2.Builder()
            .setName(name)
            .addEndpoint(
                URI.create(httpScheme + bindAddress
                    + ":" + port)).setConf(conf).setFindPort(findPort)
            .setACL(new AccessControlList(conf.get(
              YarnConfiguration.YARN_ADMIN_ACL, 
              YarnConfiguration.DEFAULT_YARN_ADMIN_ACL)))
            .setPathSpec(pathList.toArray(new String[0]));

        boolean hasSpnegoConf = spnegoPrincipalKey != null
            && conf.get(spnegoPrincipalKey) != null && spnegoKeytabKey != null
            && conf.get(spnegoKeytabKey) != null;

        if (hasSpnegoConf) {
          builder.setUsernameConfKey(spnegoPrincipalKey)
              .setKeytabConfKey(spnegoKeytabKey)
              .setSecurityEnabled(UserGroupInformation.isSecurityEnabled());
        }

        if (httpScheme.equals(WebAppUtils.HTTPS_PREFIX)) {
          WebAppUtils.loadSslConfiguration(builder, conf);
        }

        HttpServer2 server = builder.build();

        for(ServletStruct struct: servlets) {
          server.addServlet(struct.name, struct.spec, struct.clazz);
        }
        for(Map.Entry<String, Object> entry : attributes.entrySet()) {
          server.setAttribute(entry.getKey(), entry.getValue());
        }
        Map<String, String> params = getConfigParameters(csrfConfigPrefix);

        if (hasCSRFEnabled(params)) {
          LOG.info("CSRF Protection has been enabled for the {} application. "
                   + "Please ensure that there is an authentication mechanism "
                   + "enabled (kerberos, custom, etc).",
                   name);
          String restCsrfClassName = RestCsrfPreventionFilter.class.getName();
          HttpServer2.defineFilter(server.getWebAppContext(), restCsrfClassName,
                                   restCsrfClassName, params,
                                   new String[] {"/*"});
        }

        params = getConfigParameters(xfsConfigPrefix);

        if (hasXFSEnabled()) {
          String xfsClassName = XFrameOptionsFilter.class.getName();
          HttpServer2.defineFilter(server.getWebAppContext(), xfsClassName,
              xfsClassName, params,
              new String[] {"/*"});
        }

        HttpServer2.defineFilter(server.getWebAppContext(), "guice",
          GuiceFilter.class.getName(), null, new String[] { "/*" });

        webapp.setConf(conf);
        webapp.setHttpServer(server);

      } catch (ClassNotFoundException e) {
        throw new WebAppException("Error starting http server", e);
      } catch (IOException e) {
        throw new WebAppException("Error starting http server", e);
      }
      Injector injector = Guice.createInjector(webapp, new AbstractModule() {
        @Override
        protected void configure() {
          if (api != null) {
            bind(api).toInstance(application);
          }
        }
      });
      LOG.info("Registered webapp guice modules");
      // save a guice filter instance for webapp stop (mostly for unit tests)
      webapp.setGuiceFilter(injector.getInstance(GuiceFilter.class));
      if (devMode) {
        injector.getInstance(Dispatcher.class).setDevMode(devMode);
        LOG.info("in dev mode!");
      }
      return webapp;
    }

    private boolean hasCSRFEnabled(Map<String, String> params) {
      return params != null && Boolean.valueOf(params.get("enabled"));
    }

    /**
     * XFS filter is enabled by default.  If the enabled flag is not explicitly
     * specified and set to "false", this method returns true.
     * @return true if XFS is enabled, false otherwise.
     */
    private boolean hasXFSEnabled() {
      return conf.getBoolean(YarnConfiguration.YARN_XFS_ENABLED, true);
    }

    private Map<String, String> getConfigParameters(String configPrefix) {
      return configPrefix != null ? conf.getPropsWithPrefix(configPrefix) :
          null;
    }

    public WebApp start() {
      return start(null);
    }

    public WebApp start(WebApp webapp) {
      return start(webapp, null);
    }

    public WebApp start(WebApp webapp, WebAppContext ui2Context) {
      WebApp webApp = build(webapp);
      HttpServer2 httpServer = webApp.httpServer();
      if (ui2Context != null) {
        httpServer.addHandlerAtFront(ui2Context);
      }
      try {
        httpServer.start();
        LOG.info("Web app " + name + " started at "
            + httpServer.getConnectorAddress(0).getPort());
      } catch (IOException e) {
        throw new WebAppException("Error starting http server", e);
      }
      return webApp;
    }

    private String inferHostClass() {
      String thisClass = this.getClass().getName();
      Throwable t = new Throwable();
      for (StackTraceElement e : t.getStackTrace()) {
        if (e.getClassName().equals(thisClass)) continue;
        return e.getClassName();
      }
      LOG.warn("could not infer host class from", t);
      return thisClass;
    }
  }

  /**
   * Create a new webapp builder.
   * @see WebApps for a complete example
   * @param <T> application (holding the embedded webapp) type
   * @param prefix of the webapp
   * @param api the api class for the application
   * @param app the application instance
   * @param wsPrefix the prefix for the webservice api for this app
   * @return a webapp builder
   */
  public static <T> Builder<T> $for(String prefix, Class<T> api, T app, String wsPrefix) {
    return new Builder<T>(prefix, api, app, wsPrefix);
  }

  /**
   * Create a new webapp builder.
   * @see WebApps for a complete example
   * @param <T> application (holding the embedded webapp) type
   * @param prefix of the webapp
   * @param api the api class for the application
   * @param app the application instance
   * @return a webapp builder
   */
  public static <T> Builder<T> $for(String prefix, Class<T> api, T app) {
    return new Builder<T>(prefix, api, app);
  }

  // Short cut mostly for tests/demos
  @SuppressWarnings("unchecked")
  public static <T> Builder<T> $for(String prefix, T app) {
    return $for(prefix, (Class<T>)app.getClass(), app);
  }

  // Ditto
  public static <T> Builder<T> $for(T app) {
    return $for("", app);
  }

  public static <T> Builder<T> $for(String prefix) {
    return $for(prefix, null, null);
  }
}
