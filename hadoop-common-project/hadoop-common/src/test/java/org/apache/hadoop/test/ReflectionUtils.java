package org.apache.hadoop.test;

import java.lang.reflect.Field;

public class ReflectionUtils {

  public static String getStringValueOfField(Field f) throws IllegalAccessException {
    switch (f.getType().getName()) {
    case "java.lang.String":
      return (String) f.get(null);
    case "short":
      short shValue = (short) f.get(null);
      return Integer.toString(shValue);
    case "int":
      int iValue = (int) f.get(null);
      return Integer.toString(iValue);
    case "long":
      long lValue = (long) f.get(null);
      return Long.toString(lValue);
    case "float":
      float fValue = (float) f.get(null);
      return Float.toString(fValue);
    case "double":
      double dValue = (double) f.get(null);
      return Double.toString(dValue);
    case "boolean":
      boolean bValue = (boolean) f.get(null);
      return Boolean.toString(bValue);
    default:
      return null;
    }
  }
}
