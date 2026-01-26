/*
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
package org.apache.hadoop.classification.tools;

import jdk.javadoc.doclet.DocletEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Process the {@link DocletEnvironment} by substituting with (nested) proxy objects that
 * exclude elements with Private or LimitedPrivate annotations.
 * <p>
 * Based on code from http://www.sixlegs.com/blog/java/exclude-javadoc-tag.html.
 */
final class RootDocProcessor {

  private static String stability = StabilityOptions.UNSTABLE_OPTION;
  private static boolean treatUnannotatedClassesAsPrivate = false;

  static void setStability(String value) {
    stability = value;
  }

  private RootDocProcessor() {
    // no instances
  }


  static String getStability() {
    return stability;
  }

  static void setTreatUnannotatedClassesAsPrivate(boolean value) {
    treatUnannotatedClassesAsPrivate = value;
  }

  static boolean isTreatUnannotatedClassesAsPrivate() {
    return treatUnannotatedClassesAsPrivate;
  }

  public static DocletEnvironment process(DocletEnvironment root) {
    return (DocletEnvironment) wrap(root, DocletEnvironment.class);
  }

  private static final Map<Object, Object> PROXIES = new WeakHashMap<>();

  private static Object wrap(Object obj, Class<?> expectedType) {
    if (obj == null) {
      return null;
    }

    if (obj instanceof DocletEnvironment) {
      return getProxy(obj, new Class<?>[]{DocletEnvironment.class},
          new EnvHandler((DocletEnvironment) obj));
    }

    if (obj instanceof Element) {
      return getElementProxy((Element) obj);
    }

    if (obj instanceof Set) {
      return filterAndWrapIterable((Iterable<?>) obj, true);
    }
    if (obj instanceof Collection) {
      return filterAndWrapIterable((Iterable<?>) obj, false);
    }
    if (obj instanceof Iterable) {
      return filterAndWrapIterable((Iterable<?>) obj, false);
    }

    if (obj.getClass().isArray()) {
      int len = Array.getLength(obj);
      Object[] res = new Object[len];
      for (int i = 0; i < len; i++) {
        Object v = Array.get(obj, i);
        res[i] = wrap(v, v != null ? v.getClass() : Object.class);
      }
      return res;
    }

    return obj;
  }

  private static Object getElementProxy(Element el) {
    Object cached = PROXIES.get(el);
    if (cached != null) {
      return cached;
    }

    Set<Class<?>> ifaces = new LinkedHashSet<>();
    Collections.addAll(ifaces, el.getClass().getInterfaces());
    ifaces.add(Element.class);
    if (el instanceof TypeElement) {
      ifaces.add(TypeElement.class);
    }
    if (el instanceof PackageElement) {
      ifaces.add(PackageElement.class);
    }
    if (el instanceof ExecutableElement) {
      ifaces.add(ExecutableElement.class);
    }
    if (el instanceof VariableElement) {
      ifaces.add(VariableElement.class);
    }

    Object proxy = getProxy(el, ifaces.toArray(new Class<?>[0]), new ElementHandler(el));
    PROXIES.put(el, proxy);
    return proxy;
  }

  private static Object getProxy(Object target, Class<?>[] ifaces, InvocationHandler h) {
    Object cached = PROXIES.get(target);
    if (cached != null) {
      return cached;
    }
    Object p = Proxy.newProxyInstance(target.getClass().getClassLoader(), ifaces, h);
    PROXIES.put(target, p);
    return p;
  }

  @SuppressWarnings("unchecked")
  private static Object filterAndWrapIterable(Iterable<?> iterable, boolean preserveSet) {
    if (iterable == null) {
      return null;
    }
    if (preserveSet) {
      Set<Object> out = new LinkedHashSet<>();
      for (Object o : iterable) {
        if (o instanceof Element) {
          Element el = (Element) o;
          if (!exclude(el)) {
            out.add(getElementProxy(el));
          }
        } else {
          out.add(wrap(o, o != null ? o.getClass() : Object.class));
        }
      }
      return out;
    } else {
      List<Object> out = new ArrayList<>();
      for (Object o : iterable) {
        if (o instanceof Element) {
          Element el = (Element) o;
          if (!exclude(el)) {
            out.add(getElementProxy(el));
          }
        } else {
          out.add(wrap(o, o != null ? o.getClass() : Object.class));
        }
      }
      return out;
    }
  }

  private static Object unwrap(Object maybeProxy) {
    if (!(maybeProxy instanceof Proxy)) {
      return maybeProxy;
    }
    InvocationHandler ih = Proxy.getInvocationHandler(maybeProxy);
    if (ih instanceof BaseHandler) {
      return ((BaseHandler) ih).target;
    }
    return maybeProxy;
  }

  private static boolean exclude(Element el) {
    boolean sawPublic = false;

    for (AnnotationMirror am : el.getAnnotationMirrors()) {
      final String qname = am.getAnnotationType().toString();

      if (qname.equals(InterfaceAudience.Private.class.getCanonicalName())
          || qname.equals(InterfaceAudience.LimitedPrivate.class.getCanonicalName())) {
        return true;
      }

      if (stability.equals(StabilityOptions.EVOLVING_OPTION)) {
        if (qname.equals(InterfaceStability.Unstable.class.getCanonicalName())) {
          return true;
        }
      }
      if (stability.equals(StabilityOptions.STABLE_OPTION)) {
        if (qname.equals(InterfaceStability.Unstable.class.getCanonicalName())
            || qname.equals(InterfaceStability.Evolving.class.getCanonicalName())) {
          return true;
        }
      }

      if (qname.equals(InterfaceAudience.Public.class.getCanonicalName())) {
        sawPublic = true;
      }
    }

    if (sawPublic) {
      return false;
    }

    if (isTreatUnannotatedClassesAsPrivate()) {
      ElementKind k = el.getKind();
      if (k == ElementKind.CLASS || k == ElementKind.INTERFACE ||
          k == ElementKind.ANNOTATION_TYPE) {
        return true;
      }
    }

    return false;
  }

  private static abstract class BaseHandler implements InvocationHandler {
    private final Object target;

    BaseHandler(Object target) {
      this.target = target;
    }

    protected Object getTarget() {
      return target;
    }

    Object wrapReturn(Object ret) {
      if (ret == null) {
        return null;
      }
      if (ret instanceof DocletEnvironment) {
        return wrap(ret, DocletEnvironment.class);
      }
      if (ret instanceof Element) {
        return getElementProxy((Element) ret);
      }
      if (ret instanceof Set) {
        return filterAndWrapIterable((Set<?>) ret, true);
      }
      if (ret instanceof Collection) {
        return filterAndWrapIterable((Collection<?>) ret, false);
      }
      if (ret instanceof Iterable) {
        return filterAndWrapIterable((Iterable<?>) ret, false);
      }
      if (ret.getClass().isArray()) {
        return wrap(ret, ret.getClass());
      }
      return ret;
    }

    Object[] unwrapArgs(Object[] args) {
      if (args == null) {
        return null;
      }
      Object[] r = new Object[args.length];
      for (int i = 0; i < args.length; i++) {
        r[i] = unwrap(args[i]);
      }
      return r;
    }
  }

  private static final class EnvHandler extends BaseHandler {
    private final DocletEnvironment env;

    EnvHandler(DocletEnvironment env) {
      super(env);
      this.env = env;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      Object[] uargs = unwrapArgs(args);

      if ("getDocTrees".equals(name)) {
        return env.getDocTrees();
      } else if ("isIncluded".equals(name)) {
        Element e = (Element) uargs[0];
        boolean base = env.isIncluded(e);
        return base && !exclude(e);
      } else if ("getIncludedElements".equals(name)) {
        Set<? extends Element> base = env.getIncludedElements();
        return base.stream()
            .filter(e -> !exclude(e))
            .collect(Collectors.toCollection(LinkedHashSet::new));
      } else if ("getSpecifiedElements".equals(name)) {
        Set<? extends Element> base = env.getSpecifiedElements();
        return base.stream()
            .filter(e -> !exclude(e))
            .collect(Collectors.toCollection(LinkedHashSet::new));
      }

      Object ret = method.invoke(getTarget(), uargs);
      return wrapReturn(ret);
    }
  }

  private static final class ElementHandler extends BaseHandler {
    private final Element element;

    ElementHandler(Element element) {
      super(element);
      this.element = element;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      Object[] uargs = unwrapArgs(args);

      if ("equals".equals(name) && uargs != null && uargs.length == 1) {
        return Objects.equals(element, unwrap(uargs[0]));
      }
      if ("hashCode".equals(name) && (uargs == null || uargs.length == 0)) {
        return element.hashCode();
      }
      if ("toString".equals(name) && (uargs == null || uargs.length == 0)) {
        return element.toString();
      }

      if ("getEnclosedElements".equals(name) && (uargs == null || uargs.length == 0)) {
        List<? extends Element> enclosed = element.getEnclosedElements();
        List<Element> filtered = new ArrayList<>();
        for (Element e : enclosed) {
          if (!exclude(e)) {
            filtered.add(e);
          }
        }
        return filtered;
      }

      Object ret = method.invoke(getTarget(), uargs);
      return wrapReturn(ret);
    }
  }
}
