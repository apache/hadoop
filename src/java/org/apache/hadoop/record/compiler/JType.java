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

package org.apache.hadoop.record.compiler;

/**
 * Abstract Base class for all types supported by Hadoop Record I/O.
 *
 * @author Milind Bhandarkar
 */
abstract public class JType {
  
  static String toCamelCase(String name) {
    char firstChar = name.charAt(0);
    if (Character.isLowerCase(firstChar)) {
      return ""+Character.toUpperCase(firstChar) + name.substring(1);
    }
    return name;
  }
  
  JavaType javaType;
  CppType cppType;
  CType cType;
  
  abstract class JavaType {
    private String name;
    private String methodSuffix;
    private String wrapper;
    
    JavaType(String javaname,
        String suffix,
        String wrapper) {
      this.name = javaname;
      this.methodSuffix = suffix;
      this.wrapper = wrapper;
    }
    
    void genDecl(CodeBuffer cb, String fname) {
      cb.append("private "+name+" "+fname+";\n");
    }
    
    void genConstructorParam(CodeBuffer cb, String fname) {
      cb.append("final "+name+" "+fname);
    }
    
    void genGetSet(CodeBuffer cb, String fname) {
      cb.append("public "+name+" get"+toCamelCase(fname)+"() {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("public void set"+toCamelCase(fname)+"(final "+name+" "+fname+") {\n");
      cb.append("this."+fname+"="+fname+";\n");
      cb.append("}\n");
    }
    
    String getType() {
      return name;
    }
    
    String getWrapperType() {
      return wrapper;
    }
    
    String getMethodSuffix() {
      return methodSuffix;
    }
    
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      cb.append("a.write"+methodSuffix+"("+fname+",\""+tag+"\");\n");
    }
    
    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(name+" "+fname+";\n");
      }
      cb.append(fname+"=a.read"+methodSuffix+"(\""+tag+"\");\n");
    }
    
    void genCompareTo(CodeBuffer cb, String fname, String other) {
      cb.append("ret = ("+fname+" == "+other+")? 0 :(("+fname+"<"+other+
          ")?-1:1);\n");
    }
    
    abstract void genCompareBytes(CodeBuffer cb);
    
    abstract void genSlurpBytes(CodeBuffer cb, String b, String s, String l);
    
    void genEquals(CodeBuffer cb, String fname, String peer) {
      cb.append("ret = ("+fname+"=="+peer+");\n");
    }
    
    void genHashCode(CodeBuffer cb, String fname) {
      cb.append("ret = (int)"+fname+";\n");
    }
    
    void genConstructorSet(CodeBuffer cb, String fname) {
      cb.append("this."+fname+" = "+fname+";\n");
    }
    
    void genClone(CodeBuffer cb, String fname) {
      cb.append("other."+fname+" = this."+fname+";\n");
    }
  }
  
  class CppType {
    private String name;
    
    CppType(String cppname) {
      name = cppname;
    }
    
    void genDecl(CodeBuffer cb, String fname) {
      cb.append(name+" "+fname+";\n");
    }
    
    void genGetSet(CodeBuffer cb, String fname) {
      cb.append("virtual "+name+" get"+toCamelCase(fname)+"() const {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("virtual void set"+toCamelCase(fname)+"("+name+" m_) {\n");
      cb.append(fname+"=m_;\n");
      cb.append("}\n");
    }
    
    String getType() {
      return name;
    }
  }
  
  class CType {
    
  }
  
  abstract String getSignature();
  
  void setJavaType(JavaType jType) {
    this.javaType = jType;
  }
  
  JavaType getJavaType() {
    return javaType;
  }
  
  void setCppType(CppType cppType) {
    this.cppType = cppType;
  }
  
  CppType getCppType() {
    return cppType;
  }
  
  void setCType(CType cType) {
    this.cType = cType;
  }
  
  CType getCType() {
    return cType;
  }
}
