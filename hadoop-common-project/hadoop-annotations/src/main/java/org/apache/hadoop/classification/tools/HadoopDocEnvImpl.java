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

import com.sun.source.util.DocTrees;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.internal.tool.DocEnvImpl;
import jdk.javadoc.internal.tool.ToolEnvironment;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class extends Java internal DocEnvImpl to avoid cast error while
 * migrating to JDK17. It filters elements based on Hadoop's InterfaceAudience
 * and InterfaceStability annotations.
 * This class depends on JDK internal implementation, so we might need to
 * update the source code when upgrading to upper JDK versions.
 */
public class HadoopDocEnvImpl extends DocEnvImpl {

  private final DocletEnvironment delegate;
  private final String stability;
  private final boolean treatUnannotatedClassesAsPrivate;

  public HadoopDocEnvImpl(DocletEnvironment original, String stability,
      boolean treatUnannotatedClassesAsPrivate) {
    super(extractToolEnvironment(original), null);
    this.delegate = original;
    this.stability = stability;
    this.treatUnannotatedClassesAsPrivate = treatUnannotatedClassesAsPrivate;
  }

  // Use original ToolEnvironment to avoid NullPointerException
  private static ToolEnvironment extractToolEnvironment(DocletEnvironment original) {
    if (original instanceof DocEnvImpl impl) {
      return impl.toolEnv;
    }
    throw new IllegalArgumentException(
        "Expected DocEnvImpl but got: " + original.getClass().getName());
  }

  private boolean exclude(Element el) {
    boolean sawPublic = false;

    for (AnnotationMirror am : el.getAnnotationMirrors()) {
      final String qname = am.getAnnotationType().toString();

      if (qname.equals(InterfaceAudience.Private.class.getCanonicalName()) || qname.equals(
          InterfaceAudience.LimitedPrivate.class.getCanonicalName())) {
        return true;
      }

      if (stability.equals(StabilityOptions.EVOLVING_OPTION)) {
        if (qname.equals(InterfaceStability.Unstable.class.getCanonicalName())) {
          return true;
        }
      }
      if (stability.equals(StabilityOptions.STABLE_OPTION)) {
        if (qname.equals(InterfaceStability.Unstable.class.getCanonicalName()) || qname.equals(
            InterfaceStability.Evolving.class.getCanonicalName())) {
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

    if (treatUnannotatedClassesAsPrivate) {
      ElementKind k = el.getKind();
      if (k == ElementKind.CLASS || k == ElementKind.INTERFACE
          || k == ElementKind.ANNOTATION_TYPE) {
        return true;
      }
    }

    return false;
  }

  @Override
  public Set<? extends Element> getSpecifiedElements() {
    Set<? extends Element> base = delegate.getSpecifiedElements();
    return base.stream().filter(e -> !exclude(e))
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @Override
  public Set<? extends Element> getIncludedElements() {
    Set<? extends Element> base = delegate.getIncludedElements();
    return base.stream().filter(e -> !exclude(e))
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @Override
  public DocTrees getDocTrees() {
    return delegate.getDocTrees();
  }

  @Override
  public Elements getElementUtils() {
    return delegate.getElementUtils();
  }

  @Override
  public Types getTypeUtils() {
    return delegate.getTypeUtils();
  }

  @Override
  public boolean isIncluded(Element e) {
    boolean base = delegate.isIncluded(e);
    return base && !exclude(e);
  }

  @Override
  public boolean isSelected(Element e) {
    return delegate.isSelected(e);
  }

  @Override
  public JavaFileManager getJavaFileManager() {
    return delegate.getJavaFileManager();
  }

  @Override
  public SourceVersion getSourceVersion() {
    return delegate.getSourceVersion();
  }

  @Override
  public DocletEnvironment.ModuleMode getModuleMode() {
    return delegate.getModuleMode();
  }

  @Override
  public JavaFileObject.Kind getFileKind(TypeElement type) {
    return delegate.getFileKind(type);
  }
}
