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

import org.apache.hadoop.record.compiler.JCompType.CCompType;
import org.apache.hadoop.record.compiler.JCompType.CppCompType;

/**
 *
 * @author Milind Bhandarkar
 */
public class JVector extends JCompType {
  
  static private int level = 0;
  
  static private String getId(String id) { return id+getLevel(); }
  
  static private String getLevel() { return Integer.toString(level); }
  
  static private void incrLevel() { level++; }
  
  static private void decrLevel() { level--; }
  
  private JType type;
  
  class JavaVector extends JavaCompType {
    
    private JType.JavaType element;
    
    JavaVector(JType.JavaType t) {
      super("java.util.ArrayList<"+t.getWrapperType()+">",
          "Vector", "java.util.ArrayList<"+t.getWrapperType()+">");
      element = t;
    }
    
    void genCompareTo(CodeBuffer cb, String fname, String other) {
      cb.append("{\n");
      cb.append("int "+getId("len1")+" = "+fname+".size();\n");
      cb.append("int "+getId("len2")+" = "+other+".size();\n");
      cb.append("for(int "+getId("vidx")+" = 0; "+getId("vidx")+"<"+
          getId("len1")+" && "+getId("vidx")+"<"+getId("len2")+"; "+
          getId("vidx")+"++) {\n");
      cb.append(element.getType()+" "+getId("e1")+
          " = "+fname+
          ".get("+getId("vidx")+");\n");
      cb.append(element.getType()+" "+getId("e2")+
          " = "+other+
          ".get("+getId("vidx")+");\n");
      element.genCompareTo(cb, getId("e1"), getId("e2"));
      cb.append("if (ret != 0) { return ret; }\n");
      cb.append("}\n");
      cb.append("ret = ("+getId("len1")+" - "+getId("len2")+");\n");
      cb.append("}\n");
    }
    
    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(getType()+" "+fname+";\n");
      }
      cb.append("{\n");
      incrLevel();
      cb.append("org.apache.hadoop.record.Index "+getId("vidx")+" = a.startVector(\""+tag+"\");\n");
      cb.append(fname+"=new "+getType()+"();\n");
      cb.append("for (; !"+getId("vidx")+".done(); "+getId("vidx")+".incr()) {\n");
      element.genReadMethod(cb, getId("e"), getId("e"), true);
      cb.append(fname+".add("+getId("e")+");\n");
      cb.append("}\n");
      cb.append("a.endVector(\""+tag+"\");\n");
      decrLevel();
      cb.append("}\n");
    }
    
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      cb.append("{\n");
      incrLevel();
      cb.append("a.startVector("+fname+",\""+tag+"\");\n");
      cb.append("int "+getId("len")+" = "+fname+".size();\n");
      cb.append("for(int "+getId("vidx")+" = 0; "+getId("vidx")+"<"+getId("len")+"; "+getId("vidx")+"++) {\n");
      cb.append(element.getType()+" "+getId("e")+" = "+fname+".get("+getId("vidx")+");\n");
      element.genWriteMethod(cb, getId("e"), getId("e"));
      cb.append("}\n");
      cb.append("a.endVector("+fname+",\""+tag+"\");\n");
      cb.append("}\n");
      decrLevel();
    }
    
    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId("vi")+
          " = org.apache.hadoop.record.Utils.readVInt("+b+", "+s+");\n");
      cb.append("int "+getId("vz")+
          " = org.apache.hadoop.record.Utils.getVIntSize("+getId("vi")+");\n");
      cb.append(s+"+="+getId("vz")+"; "+l+"-="+getId("vz")+";\n");
      cb.append("for (int "+getId("vidx")+" = 0; "+getId("vidx")+
          " < "+getId("vi")+"; "+getId("vidx")+"++)");
      element.genSlurpBytes(cb, b,s,l);
      decrLevel();
      cb.append("}\n");
    }
    
    void genCompareBytes(CodeBuffer cb) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId("vi1")+
          " = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
      cb.append("int "+getId("vi2")+
          " = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
      cb.append("int "+getId("vz1")+
          " = org.apache.hadoop.record.Utils.getVIntSize("+getId("vi1")+");\n");
      cb.append("int "+getId("vz2")+
          " = org.apache.hadoop.record.Utils.getVIntSize("+getId("vi2")+");\n");
      cb.append("s1+="+getId("vz1")+"; s2+="+getId("vz2")+
          "; l1-="+getId("vz1")+"; l2-="+getId("vz2")+";\n");
      cb.append("for (int "+getId("vidx")+" = 0; "+getId("vidx")+
          " < "+getId("vi1")+" && "+getId("vidx")+" < "+getId("vi2")+
          "; "+getId("vidx")+"++)");
      element.genCompareBytes(cb);
      cb.append("if ("+getId("vi1")+" != "+getId("vi2")+
          ") { return ("+getId("vi1")+"<"+getId("vi2")+")?-1:0; }\n");
      decrLevel();
      cb.append("}\n");
    }
  }
  
  /** Creates a new instance of JVector */
  public JVector(JType t) {
    type = t;
    setJavaType(new JavaVector(t.getJavaType()));
    setCppType(new CppCompType(" ::std::vector<"+t.getCppType().getType()+">"));
    setCType(new CCompType());
  }
  
  String getSignature() {
    return "[" + type.getSignature() + "]";
  }
}
