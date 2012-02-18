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

package org.apache.hadoop.yarn.webapp.test;

import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.WebAppException;

import java.lang.reflect.Method;
import java.util.Map;

import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.servlet.RequestScoped;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;


import static org.mockito.Mockito.*;

public class WebAppTests {

  /**
   * Create a mock injector for tests
   * @param <T> type of class/interface
   * @param api the interface class of the object to inject
   * @param impl the implementation object to inject
   * @param modules additional guice modules
   * @return an injector
   */
  public static <T> Injector createMockInjector(final Class<T> api,
                                                final T impl,
                                                final Module... modules) {
    return Guice.createInjector(new AbstractModule() {
      final PrintWriter writer = spy(new PrintWriter(System.out));
      final HttpServletRequest request = createRequest();
      final HttpServletResponse response = createResponse();

      @Override
      protected void configure() {
        if (api != null) {
          bind(api).toInstance(impl);
        }
        bindScope(RequestScoped.class, Scopes.SINGLETON);
        if (modules != null) {
          for (Module module : modules) {
            install(module);
          }
        }
      }

      @Provides HttpServletRequest request() {
        return request;
      }

      @Provides HttpServletResponse response() {
        return response;
      }

      @Provides PrintWriter writer() {
        return writer;
      }

      HttpServletRequest createRequest() {
        // the default suffices for now
        return mock(HttpServletRequest.class);
      }

      HttpServletResponse createResponse() {
        try {
          HttpServletResponse res = mock(HttpServletResponse.class);
          when(res.getWriter()).thenReturn(writer);
          return res;
        } catch (Exception e) {
          throw new WebAppException(e);
        }
      }
    });
  }

  // convenience
  @SuppressWarnings("unchecked")
  public static <T> Injector createMockInjector(T impl) {
    return createMockInjector((Class<T>)impl.getClass(), impl);
  }

  public static void flushOutput(Injector injector) {
    HttpServletResponse res = injector.getInstance(HttpServletResponse.class);
    try {
      res.getWriter().flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> Injector testController(Class<? extends Controller> ctrlr,
      String methodName, Class<T> api, T impl, Module... modules) {
    try {
      Injector injector = createMockInjector(api, impl, modules);
      Method method = ctrlr.getMethod(methodName, (Class<?>[])null);
      method.invoke(injector.getInstance(ctrlr), (Object[])null);
      return injector;
    } catch (Exception e) {
      throw new WebAppException(e);
    }
  }

  public static <T> Injector testController(Class<? extends Controller> ctrlr,
      String methodName) {
    return testController(ctrlr, methodName, null, null);
  }

  public static <T> Injector testPage(Class<? extends View> page, Class<T> api,
                                      T impl, Map<String,String> params, Module... modules) {
    Injector injector = createMockInjector(api, impl, modules);
    View view = injector.getInstance(page);
    if(params != null) {
      for(Map.Entry<String, String> entry: params.entrySet()) {
        view.set(entry.getKey(), entry.getValue());
      }
    }
    view.render();
    flushOutput(injector);
    return injector;
  }
  
  public static <T> Injector testPage(Class<? extends View> page, Class<T> api,
                                      T impl, Module... modules) {
    return testPage(page, api, impl, null, modules);
  }

  // convenience
  public static <T> Injector testPage(Class<? extends View> page) {
    return testPage(page, null, null);
  }

  public static <T> Injector testBlock(Class<? extends SubView> block,
      Class<T> api, T impl, Module... modules) {
    Injector injector = createMockInjector(api, impl, modules);
    injector.getInstance(block).renderPartial();
    flushOutput(injector);
    return injector;
  }

  // convenience
  public static <T> Injector testBlock(Class<? extends SubView> block) {
    return testBlock(block, null, null);
  }
  
  /**
   * Convenience method to get the spy writer. 
   * @param injector the injector used for the test.
   * @return The Spy writer.
   * @throws IOException
   */
  public static PrintWriter getPrintWriter(Injector injector)
      throws IOException {
    HttpServletResponse res = injector.getInstance(HttpServletResponse.class);
    return res.getWriter();
  }
}
