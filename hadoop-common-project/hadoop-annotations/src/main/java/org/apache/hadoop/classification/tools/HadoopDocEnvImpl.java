package org.apache.hadoop.classification.tools;

import com.sun.source.util.DocTrees;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.internal.tool.DocEnvImpl;
import jdk.javadoc.internal.tool.ToolEnvironment;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import java.util.Set;

/**
 * This class extends Java internal DocEnvImpl to avoid cast error while
 * migrating to JDK17. It delegates all invocations to the proxy.
 * This class depends on JDK internal implementation, so we might need to
 * update the source code when upgrading to upper JDK versions.
 */
public class HadoopDocEnvImpl extends DocEnvImpl {

  private final DocletEnvironment proxy;

  public HadoopDocEnvImpl(DocletEnvironment original, DocletEnvironment proxy) {
    super(extractToolEnvironment(original), null);
    this.proxy = proxy;
  }

  // Use original ToolEnvironment to avoid NullPointerException
  private static ToolEnvironment extractToolEnvironment(DocletEnvironment original) {
    if (original instanceof DocEnvImpl) {
      return ((DocEnvImpl) original).toolEnv;
    }
    throw new IllegalArgumentException(
        "Expected DocEnvImpl but got: " + original.getClass().getName());
  }

  @Override
  public Set<? extends Element> getSpecifiedElements() {
    return proxy.getSpecifiedElements();
  }

  @Override
  public Set<? extends Element> getIncludedElements() {
    return proxy.getIncludedElements();
  }

  @Override
  public DocTrees getDocTrees() {
    return proxy.getDocTrees();
  }

  @Override
  public Elements getElementUtils() {
    return proxy.getElementUtils();
  }

  @Override
  public Types getTypeUtils() {
    return proxy.getTypeUtils();
  }

  @Override
  public boolean isIncluded(Element e) {
    return proxy.isIncluded(e);
  }

  @Override
  public boolean isSelected(Element e) {
    return proxy.isSelected(e);
  }

  @Override
  public JavaFileManager getJavaFileManager() {
    return proxy.getJavaFileManager();
  }

  @Override
  public SourceVersion getSourceVersion() {
    return proxy.getSourceVersion();
  }

  @Override
  public DocletEnvironment.ModuleMode getModuleMode() {
    return proxy.getModuleMode();
  }

  @Override
  public JavaFileObject.Kind getFileKind(TypeElement type) {
    return proxy.getFileKind(type);
  }
}
