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
package org.apache.hadoop.fs.compat.common;


import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.compat.HdfsCompatTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class HdfsCompatApiScope {
  static final boolean SKIP_NO_SUCH_METHOD_ERROR = true;
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsCompatApiScope.class);

  private final HdfsCompatEnvironment env;
  private final HdfsCompatSuite suite;

  public HdfsCompatApiScope(HdfsCompatEnvironment env, HdfsCompatSuite suite) {
    this.env = env;
    this.suite = suite;
  }

  public HdfsCompatReport apply() {
    List<GroupedCase> groups = collectGroup();
    HdfsCompatReport report = new HdfsCompatReport();
    for (GroupedCase group : groups) {
      if (group.methods.isEmpty()) {
        continue;
      }
      final AbstractHdfsCompatCase obj = group.obj;
      GroupedResult groupedResult = new GroupedResult(obj, group.methods);

      // SetUp
      groupedResult.setUp = test(group.setUp, obj);

      if (groupedResult.setUp == Result.OK) {
        for (Method method : group.methods) {
          CaseResult caseResult = new CaseResult();

          // Prepare
          caseResult.prepareResult = test(group.prepare, obj);

          if (caseResult.prepareResult == Result.OK) {  // Case
            caseResult.methodResult = test(method, obj);
          }

          // Cleanup
          caseResult.cleanupResult = test(group.cleanup, obj);

          groupedResult.results.put(getCaseName(method), caseResult);
        }
      }

      // TearDown
      groupedResult.tearDown = test(group.tearDown, obj);

      groupedResult.exportTo(report);
    }
    return report;
  }

  private Result test(Method method, AbstractHdfsCompatCase obj) {
    if (method == null) {  // Empty method, just OK.
      return Result.OK;
    }
    try {
      method.invoke(obj);
      return Result.OK;
    } catch (InvocationTargetException t) {
      Throwable e = t.getCause();
      if (SKIP_NO_SUCH_METHOD_ERROR && (e instanceof NoSuchMethodError)) {
        LOG.warn("Case skipped with method " + method.getName()
            + " of class " + obj.getClass(), e);
        return Result.SKIP;
      } else {
        LOG.warn("Case failed with method " + method.getName()
            + " of class " + obj.getClass(), e);
        return Result.ERROR;
      }
    } catch (ReflectiveOperationException e) {
      LOG.error("Illegal Compatibility Case method " + method.getName()
          + " of class " + obj.getClass(), e);
      throw new HdfsCompatIllegalCaseException(e.getMessage());
    }
  }

  private List<GroupedCase> collectGroup() {
    Class<? extends AbstractHdfsCompatCase>[] cases = suite.getApiCases();
    List<GroupedCase> groups = new ArrayList<>();
    for (Class<? extends AbstractHdfsCompatCase> cls : cases) {
      try {
        groups.add(GroupedCase.parse(cls, this.env));
      } catch (ReflectiveOperationException e) {
        LOG.error("Illegal Compatibility Group " + cls.getName(), e);
        throw new HdfsCompatIllegalCaseException(e.getMessage());
      }
    }
    return groups;
  }

  private static String getCaseName(Method caseMethod) {
    HdfsCompatCase annotation = caseMethod.getAnnotation(HdfsCompatCase.class);
    assert (annotation != null);
    if (annotation.brief().isEmpty()) {
      return caseMethod.getName();
    } else {
      return caseMethod.getName() + " (" + annotation.brief() + ")";
    }
  }

  @VisibleForTesting
  public static Set<String> getPublicInterfaces(Class<?> cls) {
    Method[] methods = cls.getDeclaredMethods();
    Set<String> publicMethodNames = new HashSet<>();
    for (Method method : methods) {
      int modifiers = method.getModifiers();
      if (Modifier.isPublic(modifiers) && !Modifier.isStatic(modifiers)) {
        publicMethodNames.add(method.getName());
      }
    }
    publicMethodNames.remove(cls.getSimpleName());
    publicMethodNames.remove("toString");
    return publicMethodNames;
  }

  private static final class GroupedCase {
    private static final Map<String, Set<String>> DEFINED_METHODS =
        new HashMap<>();
    private final AbstractHdfsCompatCase obj;
    private final List<Method> methods;
    private final Method setUp;
    private final Method tearDown;
    private final Method prepare;
    private final Method cleanup;

    private GroupedCase(AbstractHdfsCompatCase obj, List<Method> methods,
                        Method setUp, Method tearDown,
                        Method prepare, Method cleanup) {
      this.obj = obj;
      this.methods = methods;
      this.setUp = setUp;
      this.tearDown = tearDown;
      this.prepare = prepare;
      this.cleanup = cleanup;
    }

    private static GroupedCase parse(Class<? extends AbstractHdfsCompatCase> cls,
                                     HdfsCompatEnvironment env)
        throws ReflectiveOperationException {
      Constructor<? extends AbstractHdfsCompatCase> ctor = cls.getConstructor();
      ctor.setAccessible(true);
      AbstractHdfsCompatCase caseObj = ctor.newInstance();
      caseObj.init(env);
      Method[] declaredMethods = caseObj.getClass().getDeclaredMethods();
      List<Method> caseMethods = new ArrayList<>();
      Method setUp = null;
      Method tearDown = null;
      Method prepare = null;
      Method cleanup = null;
      for (Method method : declaredMethods) {
        if (method.isAnnotationPresent(HdfsCompatCase.class)) {
          if (method.isAnnotationPresent(HdfsCompatCaseSetUp.class) ||
              method.isAnnotationPresent(HdfsCompatCaseTearDown.class) ||
              method.isAnnotationPresent(HdfsCompatCasePrepare.class) ||
              method.isAnnotationPresent(HdfsCompatCaseCleanup.class)) {
            throw new HdfsCompatIllegalCaseException(
                "Compatibility Case must not be annotated by" +
                    " Prepare/Cleanup or SetUp/TearDown");
          }
          HdfsCompatCase annotation = method.getAnnotation(HdfsCompatCase.class);
          if (annotation.ifDef().isEmpty()) {
            caseMethods.add(method);
          } else {
            String[] requireDefined = annotation.ifDef().split(",");
            if (Arrays.stream(requireDefined).allMatch(GroupedCase::checkDefined)) {
              caseMethods.add(method);
            }
          }
        } else {
          if (method.isAnnotationPresent(HdfsCompatCaseSetUp.class)) {
            if (setUp != null) {
              throw new HdfsCompatIllegalCaseException(
                  "Duplicate SetUp method in Compatibility Case");
            }
            setUp = method;
          }
          if (method.isAnnotationPresent(HdfsCompatCaseTearDown.class)) {
            if (tearDown != null) {
              throw new HdfsCompatIllegalCaseException(
                  "Duplicate TearDown method in Compatibility Case");
            }
            tearDown = method;
          }
          if (method.isAnnotationPresent(HdfsCompatCasePrepare.class)) {
            if (prepare != null) {
              throw new HdfsCompatIllegalCaseException(
                  "Duplicate Prepare method in Compatibility Case");
            }
            prepare = method;
          }
          if (method.isAnnotationPresent(HdfsCompatCaseCleanup.class)) {
            if (cleanup != null) {
              throw new HdfsCompatIllegalCaseException(
                  "Duplicate Cleanup method in Compatibility Case");
            }
            cleanup = method;
          }
        }
      }
      return new GroupedCase(caseObj, caseMethods,
          setUp, tearDown, prepare, cleanup);
    }

    private static synchronized boolean checkDefined(String ifDef) {
      String[] classAndMethod = ifDef.split("#", 2);
      if (classAndMethod.length < 2) {
        throw new HdfsCompatIllegalCaseException(
            "ifDef must be with format className#methodName");
      }
      final String className = classAndMethod[0];
      final String methodName = classAndMethod[1];
      Set<String> methods = DEFINED_METHODS.getOrDefault(className, null);
      if (methods != null) {
        return methods.contains(methodName);
      }
      Class<?> cls;
      try {
        cls = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new HdfsCompatIllegalCaseException(e.getMessage());
      }
      methods = getPublicInterfaces(cls);
      DEFINED_METHODS.put(className, methods);
      return methods.contains(methodName);
    }
  }

  private static final class GroupedResult {
    private static final int COMMON_PREFIX_LEN = HdfsCompatTool.class
        .getPackage().getName().length() + ".cases.".length();
    private final String prefix;
    private Result setUp;
    private Result tearDown;
    private final LinkedHashMap<String, CaseResult> results;

    private GroupedResult(AbstractHdfsCompatCase obj, List<Method> methods) {
      this.prefix = getNamePrefix(obj.getClass());
      this.results = new LinkedHashMap<>();
      for (Method method : methods) {
        this.results.put(getCaseName(method), new CaseResult());
      }
    }

    private void exportTo(HdfsCompatReport report) {
      if (this.setUp == Result.SKIP) {
        List<String> cases = results.keySet().stream().map(m -> prefix + m)
            .collect(Collectors.toList());
        report.addSkippedCase(cases);
        return;
      }
      if ((this.setUp == Result.ERROR) || (this.tearDown == Result.ERROR)) {
        List<String> cases = results.keySet().stream().map(m -> prefix + m)
            .collect(Collectors.toList());
        report.addFailedCase(cases);
        return;
      }

      List<String> passed = new ArrayList<>();
      List<String> failed = new ArrayList<>();
      List<String> skipped = new ArrayList<>();
      for (Map.Entry<String, CaseResult> entry : results.entrySet()) {
        final String caseName = prefix + entry.getKey();
        CaseResult result = entry.getValue();
        if (result.prepareResult == Result.SKIP) {
          skipped.add(caseName);
          continue;
        }
        if ((result.prepareResult == Result.ERROR) ||
            (result.cleanupResult == Result.ERROR) ||
            (result.methodResult == Result.ERROR)) {
          failed.add(caseName);
        } else if (result.methodResult == Result.OK) {
          passed.add(caseName);
        } else {
          skipped.add(caseName);
        }
      }

      if (!passed.isEmpty()) {
        report.addPassedCase(passed);
      }
      if (!failed.isEmpty()) {
        report.addFailedCase(failed);
      }
      if (!skipped.isEmpty()) {
        report.addSkippedCase(skipped);
      }
    }

    private static String getNamePrefix(Class<? extends AbstractHdfsCompatCase> cls) {
      return (cls.getPackage().getName() + ".").substring(COMMON_PREFIX_LEN) +
          getGroupName(cls) + ".";
    }

    private static String getGroupName(Class<? extends AbstractHdfsCompatCase> cls) {
      if (cls.isAnnotationPresent(HdfsCompatCaseGroup.class)) {
        HdfsCompatCaseGroup annotation = cls.getAnnotation(HdfsCompatCaseGroup.class);
        if (!annotation.name().isEmpty()) {
          return annotation.name();
        }
      }
      return cls.getSimpleName();
    }
  }

  private static class CaseResult {
    private Result prepareResult = Result.SKIP;
    private Result cleanupResult = Result.SKIP;
    private Result methodResult = Result.SKIP;
  }

  private enum Result {
    OK,
    ERROR,
    SKIP,
  }
}