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

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ConstructorDoc;
import com.sun.javadoc.Doc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ProgramElementDoc;
import com.sun.javadoc.RootDoc;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Process the {@link RootDoc} by substituting with (nested) proxy objects that
 * exclude elements with Private or LimitedPrivate annotations.
 * <p>
 * Based on code from http://www.sixlegs.com/blog/java/exclude-javadoc-tag.html.
 */
class RootDocProcessor {
  
  static String stability = StabilityOptions.UNSTABLE_OPTION;
  static boolean treatUnannotatedClassesAsPrivate = false;
  
  public static RootDoc process(RootDoc root) {
    return (RootDoc) process(root, RootDoc.class);
  }
  
  private static Object process(Object obj, Class<?> type) { 
    if (obj == null) { 
      return null; 
    } 
    Class<?> cls = obj.getClass(); 
    if (cls.getName().startsWith("com.sun.")) { 
      return getProxy(obj); 
    } else if (obj instanceof Object[]) { 
      Class<?> componentType = type.isArray() ? type.getComponentType() 
	  : cls.getComponentType();
      Object[] array = (Object[]) obj;
      Object[] newArray = (Object[]) Array.newInstance(componentType,
	  array.length); 
      for (int i = 0; i < array.length; ++i) {
        newArray[i] = process(array[i], componentType);
      }
      return newArray;
    } 
    return obj; 
  }
  
  private static Map<Object, Object> proxies =
    new WeakHashMap<Object, Object>(); 
  
  private static Object getProxy(Object obj) { 
    Object proxy = proxies.get(obj); 
    if (proxy == null) { 
      proxy = Proxy.newProxyInstance(obj.getClass().getClassLoader(), 
        obj.getClass().getInterfaces(), new ExcludeHandler(obj)); 
      proxies.put(obj, proxy); 
    } 
    return proxy; 
  } 

  private static class ExcludeHandler implements InvocationHandler {
    private Object target;

    public ExcludeHandler(Object target) {
      this.target = target;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
	throws Throwable {
      String methodName = method.getName();
      if (target instanceof Doc) {
	if (methodName.equals("isIncluded")) {
	  Doc doc = (Doc) target;
	  return !exclude(doc) && doc.isIncluded();
	}
	if (target instanceof RootDoc) {
	  if (methodName.equals("classes")) {
	    return filter(((RootDoc) target).classes(), ClassDoc.class);
	  } else if (methodName.equals("specifiedClasses")) {
	    return filter(((RootDoc) target).specifiedClasses(), ClassDoc.class);
	  } else if (methodName.equals("specifiedPackages")) {
	    return filter(((RootDoc) target).specifiedPackages(), PackageDoc.class);
	  }
	} else if (target instanceof ClassDoc) {
	  if (isFiltered(args)) {
	    if (methodName.equals("methods")) {
	      return filter(((ClassDoc) target).methods(true), MethodDoc.class);
	    } else if (methodName.equals("fields")) {
	      return filter(((ClassDoc) target).fields(true), FieldDoc.class);
	    } else if (methodName.equals("innerClasses")) {
	      return filter(((ClassDoc) target).innerClasses(true),
		  ClassDoc.class);
	    } else if (methodName.equals("constructors")) {
	      return filter(((ClassDoc) target).constructors(true),
		  ConstructorDoc.class);
	    }
	  }
	} else if (target instanceof PackageDoc) {
	  if (methodName.equals("allClasses")) {
	    if (isFiltered(args)) {
	      return filter(((PackageDoc) target).allClasses(true),
		ClassDoc.class);
	    } else {
	      return filter(((PackageDoc) target).allClasses(), ClassDoc.class);  
	    }
	  } else if (methodName.equals("annotationTypes")) {
	    return filter(((PackageDoc) target).annotationTypes(),
		AnnotationTypeDoc.class);
	  } else if (methodName.equals("enums")) {
	    return filter(((PackageDoc) target).enums(),
		ClassDoc.class);
	  } else if (methodName.equals("errors")) {
	    return filter(((PackageDoc) target).errors(),
		ClassDoc.class);
	  } else if (methodName.equals("exceptions")) {
	    return filter(((PackageDoc) target).exceptions(),
		ClassDoc.class);
	  } else if (methodName.equals("interfaces")) {
	    return filter(((PackageDoc) target).interfaces(),
		ClassDoc.class);
	  } else if (methodName.equals("ordinaryClasses")) {
	    return filter(((PackageDoc) target).ordinaryClasses(),
		ClassDoc.class);
	  }
	}
      }

      if (args != null) {
	if (methodName.equals("compareTo") || methodName.equals("equals")
	    || methodName.equals("overrides")
	    || methodName.equals("subclassOf")) {
	  args[0] = unwrap(args[0]);
	}
      }
      try {
	return process(method.invoke(target, args), method.getReturnType());
      } catch (InvocationTargetException e) {
	throw e.getTargetException();
      }
    }
      
    private static boolean exclude(Doc doc) {
      AnnotationDesc[] annotations = null;
      if (doc instanceof ProgramElementDoc) {
	annotations = ((ProgramElementDoc) doc).annotations();
      } else if (doc instanceof PackageDoc) {
	annotations = ((PackageDoc) doc).annotations();
      }
      if (annotations != null) {
	for (AnnotationDesc annotation : annotations) {
	  String qualifiedTypeName = annotation.annotationType().qualifiedTypeName();
	  if (qualifiedTypeName.equals(
	        InterfaceAudience.Private.class.getCanonicalName())
	    || qualifiedTypeName.equals(
                InterfaceAudience.LimitedPrivate.class.getCanonicalName())) {
	    return true;
	  }
	  if (stability.equals(StabilityOptions.EVOLVING_OPTION)) {
	    if (qualifiedTypeName.equals(
		InterfaceStability.Unstable.class.getCanonicalName())) {
	      return true;
	    }
	  }
	  if (stability.equals(StabilityOptions.STABLE_OPTION)) {
	    if (qualifiedTypeName.equals(
		InterfaceStability.Unstable.class.getCanonicalName())
              || qualifiedTypeName.equals(
  		InterfaceStability.Evolving.class.getCanonicalName())) {
	      return true;
	    }
	  }
	}
        for (AnnotationDesc annotation : annotations) {
          String qualifiedTypeName =
            annotation.annotationType().qualifiedTypeName();
          if (qualifiedTypeName.equals(
              InterfaceAudience.Public.class.getCanonicalName())) {
            return false;
          }
        }
      }
      if (treatUnannotatedClassesAsPrivate) {
        return doc.isClass() || doc.isInterface() || doc.isAnnotationType();
      }
      return false;
    }
      
    private static Object[] filter(Doc[] array, Class<?> componentType) {
      if (array == null || array.length == 0) {
	return array;
      }
      List<Object> list = new ArrayList<Object>(array.length);
      for (Doc entry : array) {
	if (!exclude(entry)) {
	  list.add(process(entry, componentType));
	}
      }
      return list.toArray((Object[]) Array.newInstance(componentType, list
	  .size()));
    }

    private Object unwrap(Object proxy) {
      if (proxy instanceof Proxy)
	return ((ExcludeHandler) Proxy.getInvocationHandler(proxy)).target;
      return proxy;
    }
      
    private boolean isFiltered(Object[] args) {
      return args != null && Boolean.TRUE.equals(args[0]);
    }

  }

}
