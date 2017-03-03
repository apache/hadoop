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
import static com.google.common.base.Preconditions.checkState;
import static org.apache.hadoop.yarn.util.StringHelper.djoin;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.util.StringHelper.pjoin;

import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Manages path info to controller#action routing.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
class Router {
  static final Logger LOG = LoggerFactory.getLogger(Router.class);
  static final ImmutableList<String> EMPTY_LIST = ImmutableList.of();
  static final CharMatcher SLASH = CharMatcher.is('/');
  static final Pattern controllerRe =
      Pattern.compile("^/[A-Za-z_]\\w*(?:/.*)?");

  static class Dest {
    final String prefix;
    final ImmutableList<String> pathParams;
    final Method action;
    final Class<? extends Controller> controllerClass;
    Class<? extends View> defaultViewClass;
    final EnumSet<WebApp.HTTP> methods;

    Dest(String path, Method method, Class<? extends Controller> cls,
         List<String> pathParams, WebApp.HTTP httpMethod) {
      prefix = checkNotNull(path);
      action = checkNotNull(method);
      controllerClass = checkNotNull(cls);
      this.pathParams = pathParams != null ? ImmutableList.copyOf(pathParams)
                                           : EMPTY_LIST;
      methods = EnumSet.of(httpMethod);
    }
  }

  Class<?> hostClass; // starting point to look for default classes

  final TreeMap<String, Dest> routes = Maps.newTreeMap(); // path->dest

  synchronized Dest add(WebApp.HTTP httpMethod, String path,
                        Class<? extends Controller> cls,
                        String action, List<String> names){
    return addWithOptionalDefaultView(
        httpMethod, path, cls, action, names, true);
  }

  synchronized Dest addWithoutDefaultView(WebApp.HTTP httpMethod,
      String path, Class<? extends Controller> cls, String action,
      List<String> names){
    return addWithOptionalDefaultView(httpMethod, path, cls, action,
        names, false);
  }
  /**
   * Add a route to the router.
   * e.g., add(GET, "/foo/show", FooController.class, "show", [name...]);
   * The name list is from /foo/show/:name/...
   */
  synchronized Dest addWithOptionalDefaultView(WebApp.HTTP httpMethod,
      String path, Class<? extends Controller> cls,
      String action, List<String> names, boolean defaultViewNeeded) {
    LOG.debug("adding {}({})->{}#{}", new Object[]{path, names, cls, action});
    Dest dest = addController(httpMethod, path, cls, action, names);
    if (defaultViewNeeded) {
      addDefaultView(dest);
    }
    return dest;
  }

  private Dest addController(WebApp.HTTP httpMethod, String path,
                             Class<? extends Controller> cls,
                             String action, List<String> names) {
    try {
      // Look for the method in all public methods declared in the class
      // or inherited by the class.
      // Note: this does not distinguish methods with the same signature
      // but different return types.
      // TODO: We may want to deal with methods that take parameters in the future
      Method method = cls.getMethod(action);
      Dest dest = routes.get(path);
      if (dest == null) {
        method.setAccessible(true); // avoid any runtime checks
        dest = new Dest(path, method, cls, names, httpMethod);
        routes.put(path, dest);
        return dest;
      }
      dest.methods.add(httpMethod);
      return dest;
    } catch (NoSuchMethodException nsme) {
      throw new WebAppException(action + "() not found in " + cls);
    } catch (SecurityException se) {
      throw new WebAppException("Security exception thrown for " + action +
        "() in " + cls);
    }
  }

  private void addDefaultView(Dest dest) {
    String controllerName = dest.controllerClass.getSimpleName();
    if (controllerName.endsWith("Controller")) {
      controllerName = controllerName.substring(0,
          controllerName.length() - 10);
    }
    dest.defaultViewClass = find(View.class,
                                 dest.controllerClass.getPackage().getName(),
                                 join(controllerName + "View"));
  }

  void setHostClass(Class<?> cls) {
    hostClass = cls;
  }

  /**
   * Resolve a path to a destination.
   */
  synchronized Dest resolve(String httpMethod, String path) {
    WebApp.HTTP method = WebApp.HTTP.valueOf(httpMethod); // can throw
    Dest dest = lookupRoute(method, path);
    if (dest == null) {
      return resolveDefault(method, path);
    }
    return dest;
  }

  private Dest lookupRoute(WebApp.HTTP method, String path) {
    String key = path;
    do {
      Dest dest = routes.get(key);
      if (dest != null && methodAllowed(method, dest)) {
        if ((Object)key == path) { // shut up warnings
          LOG.debug("exact match for {}: {}", key, dest.action);
          return dest;
        } else if (isGoodMatch(dest, path)) {
          LOG.debug("prefix match2 for {}: {}", key, dest.action);
          return dest;
        }
        return resolveAction(method, dest, path);
      }
      Map.Entry<String, Dest> lower = routes.lowerEntry(key);
      if (lower == null) {
        return null;
      }
      dest = lower.getValue();
      if (prefixMatches(dest, path)) {
        if (methodAllowed(method, dest)) {
          if (isGoodMatch(dest, path)) {
            LOG.debug("prefix match for {}: {}", lower.getKey(), dest.action);
            return dest;
          }
          return resolveAction(method, dest, path);
        }
        // check other candidates
        int slashPos = key.lastIndexOf('/');
        key = slashPos > 0 ? path.substring(0, slashPos) : "/";
      } else {
        key = "/";
      }
    } while (true);
  }

  static boolean methodAllowed(WebApp.HTTP method, Dest dest) {
    // Accept all methods by default, unless explicity configured otherwise.
    return dest.methods.contains(method) || (dest.methods.size() == 1 &&
           dest.methods.contains(WebApp.HTTP.GET));
  }

  static boolean prefixMatches(Dest dest, String path) {
    LOG.debug("checking prefix {}{} for path: {}", new Object[]{dest.prefix,
              dest.pathParams, path});
    if (!path.startsWith(dest.prefix)) {
      return false;
    }
    int prefixLen = dest.prefix.length();
    if (prefixLen > 1 && path.length() > prefixLen &&
        path.charAt(prefixLen) != '/') {
      return false;
    }
    // prefix is / or prefix is path or prefix/...
    return true;
  }

  static boolean isGoodMatch(Dest dest, String path) {
    if (SLASH.countIn(dest.prefix) > 1) {
      return true;
    }
    // We want to match (/foo, :a) for /foo/bar/blah and (/, :a) for /123
    // but NOT / for /foo or (/, :a) for /foo or /foo/ because default route
    // (FooController#index) for /foo and /foo/ takes precedence.
    if (dest.prefix.length() == 1) {
      return dest.pathParams.size() > 0 && !maybeController(path);
    }
    return dest.pathParams.size() > 0 || // /foo should match /foo/
        (path.endsWith("/") && SLASH.countIn(path) == 2);
  }

  static boolean maybeController(String path) {
    return controllerRe.matcher(path).matches();
  }

  // Assume /controller/action style path
  private Dest resolveDefault(WebApp.HTTP method, String path) {
    List<String> parts = WebApp.parseRoute(path);
    String controller = parts.get(WebApp.R_CONTROLLER);
    String action = parts.get(WebApp.R_ACTION);
    // NameController is encouraged default
    Class<? extends Controller> cls = find(Controller.class,
                                           join(controller, "Controller"));
    if (cls == null) {
      cls = find(Controller.class, controller);
    }
    if (cls == null) {
      throw new WebAppException(join(path, ": controller for ", controller,
                                " not found"));
    }
    return add(method, defaultPrefix(controller, action), cls, action, null);
  }

  private String defaultPrefix(String controller, String action) {
    if (controller.equals("default") && action.equals("index")) {
      return "/";
    }
    if (action.equals("index")) {
      return join('/', controller);
    }
    return pjoin("", controller, action);
  }

  private <T> Class<? extends T> find(Class<T> cls, String cname) {
    String pkg = hostClass.getPackage().getName();
    return find(cls, pkg, cname);
  }

  private <T> Class<? extends T> find(Class<T> cls, String pkg, String cname) {
    String name = StringUtils.capitalize(cname);
    Class<? extends T> found = load(cls, djoin(pkg, name));
    if (found == null) {
      found = load(cls, djoin(pkg, "webapp", name));
    }
    if (found == null) {
      found = load(cls, join(hostClass.getName(), '$', name));
    }
    return found;
  }

  @SuppressWarnings("unchecked")
  private <T> Class<? extends T> load(Class<T> cls, String className) {
    LOG.debug("trying: {}", className);
    try {
      Class<?> found = Class.forName(className);
      if (cls.isAssignableFrom(found)) {
        LOG.debug("found {}", className);
        return (Class<? extends T>) found;
      }
      LOG.warn("found a {} but it's not a {}", className, cls.getName());
    } catch (ClassNotFoundException e) {
      // OK in this case.
    }
    return null;
  }

  // Dest may contain a candidate controller
  private Dest resolveAction(WebApp.HTTP method, Dest dest, String path) {
    if (dest.prefix.length() == 1) {
      return null;
    }
    checkState(!isGoodMatch(dest, path), dest.prefix);
    checkState(SLASH.countIn(path) > 1, path);
    List<String> parts = WebApp.parseRoute(path);
    String controller = parts.get(WebApp.R_CONTROLLER);
    String action = parts.get(WebApp.R_ACTION);
    return add(method, pjoin("", controller, action), dest.controllerClass,
               action, null);
  }
}
