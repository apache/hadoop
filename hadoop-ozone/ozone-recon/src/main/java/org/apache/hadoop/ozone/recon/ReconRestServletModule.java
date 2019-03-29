/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.internal.inject.InjectionManager;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;
import org.glassfish.jersey.servlet.ServletContainer;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.servlet.ServletModule;

/**
 * Class to scan API Service classes and bind them to the injector.
 */
public abstract class ReconRestServletModule extends ServletModule {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconRestServletModule.class);

  @Override
  abstract protected void configureServlets();

  /**
   * Interface to provide packages for scanning.
   */
  public interface RestKeyBindingBuilder {
    void packages(String... packages);
  }

  protected RestKeyBindingBuilder rest(String... urlPatterns) {
    return new RestKeyBindingBuilderImpl(Arrays.asList(urlPatterns));
  }

  private class RestKeyBindingBuilderImpl implements RestKeyBindingBuilder {
    private List<String> paths;

    RestKeyBindingBuilderImpl(List<String> paths) {
      this.paths = paths;
    }

    private void checkIfPackageExistsAndLog(String pkg) {
      String resourcePath = pkg.replace(".", "/");
      URL resource = getClass().getClassLoader().getResource(resourcePath);
      if (resource != null) {
        LOG.info("rest(" + paths + ").packages(" + pkg + ")");
      } else {
        LOG.info("No Beans in '" + pkg + "' found. Requests " + paths
            + " will fail.");
      }
    }

    @Override
    public void packages(String... packages) {
      StringBuilder sb = new StringBuilder();

      for (String pkg : packages) {
        if (sb.length() > 0) {
          sb.append(',');
        }
        checkIfPackageExistsAndLog(pkg);
        sb.append(pkg);
      }
      Map<String, String> params = new HashMap<>();
      params.put("javax.ws.rs.Application",
          GuiceResourceConfig.class.getCanonicalName());
      if (sb.length() > 0) {
        params.put("jersey.config.server.provider.packages", sb.toString());
      }
      bind(ServletContainer.class).in(Scopes.SINGLETON);
      for (String path : paths) {
        serve(path).with(ServletContainer.class, params);
      }
    }
  }
}

/**
 * Class to bridge Guice bindings to Jersey hk2 bindings.
 */
class GuiceResourceConfig extends ResourceConfig {
  GuiceResourceConfig() {
    register(new ContainerLifecycleListener() {
      public void onStartup(Container container) {
        ServletContainer servletContainer = (ServletContainer) container;
        InjectionManager injectionManager = container.getApplicationHandler()
            .getInjectionManager();
        ServiceLocator serviceLocator = injectionManager
            .getInstance(ServiceLocator.class);
        GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);
        GuiceIntoHK2Bridge guiceBridge = serviceLocator
            .getService(GuiceIntoHK2Bridge.class);
        Injector injector = (Injector) servletContainer.getServletContext()
            .getAttribute(Injector.class.getName());
        guiceBridge.bridgeGuiceInjector(injector);
      }

      public void onReload(Container container) {
      }

      public void onShutdown(Container container) {
      }
    });
  }
}
