package org.apache.hadoop.hbase.util;

/**
 * Utilities for class manipulation.
 */
public class Classes {

  /**
   * Equivalent of {@link Class#forName(String)} which also returns classes for
   * primitives like <code>boolean</code>, etc.
   * 
   * @param className
   *          The name of the class to retrieve. Can be either a normal class or
   *          a primitive class.
   * @return The class specified by <code>className</code>
   * @throws ClassNotFoundException
   *           If the requested class can not be found.
   */
  public static Class<?> extendedForName(String className)
      throws ClassNotFoundException {
    Class<?> valueType;
    if (className.equals("boolean")) {
      valueType = boolean.class;
    } else if (className.equals("byte")) {
      valueType = byte.class;
    } else if (className.equals("short")) {
      valueType = short.class;
    } else if (className.equals("int")) {
      valueType = int.class;
    } else if (className.equals("long")) {
      valueType = long.class;
    } else if (className.equals("float")) {
      valueType = float.class;
    } else if (className.equals("double")) {
      valueType = double.class;
    } else if (className.equals("char")) {
      valueType = char.class;
    } else {
      valueType = Class.forName(className);
    }
    return valueType;
  }

}
