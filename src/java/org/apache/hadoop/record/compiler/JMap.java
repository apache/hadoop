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
 */
public class JMap extends JCompType {
  
  static private int level = 0;
  
  static private String getLevel() { return Integer.toString(level); }
  
  static private void incrLevel() { level++; }
  
  static private void decrLevel() { level--; }
  
  static private String getId(String id) { return id+getLevel(); }
  
  private JType keyType;
  private JType valueType;
  
  class JavaMap extends JavaCompType {
    
    JType.JavaType key;
    JType.JavaType value;
    
    JavaMap(JType.JavaType key, JType.JavaType value) {
      super("java.util.TreeMap<"+key.getWrapperType()+","+value.getWrapperType()+">",
            "Map",
            "java.util.TreeMap<"+key.getWrapperType()+","+value.getWrapperType()+">");
      this.key = key;
      this.value = value;
    }
    
    void genCompareTo(CodeBuffer cb, String fname, String other) {
      String setType = "java.util.Set<"+key.getWrapperType()+"> ";
      String iterType = "java.util.Iterator<"+key.getWrapperType()+"> ";
      cb.append("{\n");
      cb.append(setType+getId("set1")+" = "+fname+".keySet();\n");
      cb.append(setType+getId("set2")+" = "+other+".keySet();\n");
      cb.append(iterType+getId("miter1")+" = "+
                getId("set1")+".iterator();\n");
      cb.append(iterType+getId("miter2")+" = "+
                getId("set2")+".iterator();\n");
      cb.append("for(; "+getId("miter1")+".hasNext() && "+
                getId("miter2")+".hasNext();) {\n");
      cb.append(key.getType()+" "+getId("k1")+
                " = "+getId("miter1")+".next();\n");
      cb.append(key.getType()+" "+getId("k2")+
                " = "+getId("miter2")+".next();\n");
      key.genCompareTo(cb, getId("k1"), getId("k2"));
      cb.append("if (ret != 0) { return ret; }\n");
      cb.append("}\n");
      cb.append("ret = ("+getId("set1")+".size() - "+getId("set2")+".size());\n");
      cb.append("}\n");
    }
    
    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(getType()+" "+fname+";\n");
      }
      cb.append("{\n");
      incrLevel();
      cb.append("org.apache.hadoop.record.Index "+getId("midx")+" = a.startMap(\""+tag+"\");\n");
      cb.append(fname+"=new "+getType()+"();\n");
      cb.append("for (; !"+getId("midx")+".done(); "+getId("midx")+".incr()) {\n");
      key.genReadMethod(cb, getId("k"),getId("k"), true);
      value.genReadMethod(cb, getId("v"), getId("v"), true);
      cb.append(fname+".put("+getId("k")+","+getId("v")+");\n");
      cb.append("}\n");
      cb.append("a.endMap(\""+tag+"\");\n");
      decrLevel();
      cb.append("}\n");
    }
    
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      String setType = "java.util.Set<java.util.Map.Entry<"+
        key.getWrapperType()+","+value.getWrapperType()+">> ";
      String entryType = "java.util.Map.Entry<"+
        key.getWrapperType()+","+value.getWrapperType()+"> ";
      String iterType = "java.util.Iterator<java.util.Map.Entry<"+
        key.getWrapperType()+","+value.getWrapperType()+">> ";
      cb.append("{\n");
      incrLevel();
      cb.append("a.startMap("+fname+",\""+tag+"\");\n");
      cb.append(setType+getId("es")+" = "+fname+".entrySet();\n");
      cb.append("for("+iterType+getId("midx")+" = "+getId("es")+".iterator(); "+getId("midx")+".hasNext();) {\n");
      cb.append(entryType+getId("me")+" = "+getId("midx")+".next();\n");
      cb.append(key.getType()+" "+getId("k")+" = "+getId("me")+".getKey();\n");
      cb.append(value.getType()+" "+getId("v")+" = "+getId("me")+".getValue();\n");
      key.genWriteMethod(cb, getId("k"), getId("k"));
      value.genWriteMethod(cb, getId("v"), getId("v"));
      cb.append("}\n");
      cb.append("a.endMap("+fname+",\""+tag+"\");\n");
      cb.append("}\n");
      decrLevel();
    }
    
    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId("mi")+
                " = org.apache.hadoop.record.Utils.readVInt("+b+", "+s+");\n");
      cb.append("int "+getId("mz")+
                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("mi")+");\n");
      cb.append(s+"+="+getId("mz")+"; "+l+"-="+getId("mz")+";\n");
      cb.append("for (int "+getId("midx")+" = 0; "+getId("midx")+
                " < "+getId("mi")+"; "+getId("midx")+"++) {");
      key.genSlurpBytes(cb, b, s, l);
      value.genSlurpBytes(cb, b, s, l);
      cb.append("}\n");
      decrLevel();
      cb.append("}\n");
    }
    
    void genCompareBytes(CodeBuffer cb) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId("mi1")+
                " = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
      cb.append("int "+getId("mi2")+
                " = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
      cb.append("int "+getId("mz1")+
                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("mi1")+");\n");
      cb.append("int "+getId("mz2")+
                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("mi2")+");\n");
      cb.append("s1+="+getId("mz1")+"; s2+="+getId("mz2")+
                "; l1-="+getId("mz1")+"; l2-="+getId("mz2")+";\n");
      cb.append("for (int "+getId("midx")+" = 0; "+getId("midx")+
                " < "+getId("mi1")+" && "+getId("midx")+" < "+getId("mi2")+
                "; "+getId("midx")+"++) {");
      key.genCompareBytes(cb);
      value.genSlurpBytes(cb, "b1", "s1", "l1");
      value.genSlurpBytes(cb, "b2", "s2", "l2");
      cb.append("}\n");
      cb.append("if ("+getId("mi1")+" != "+getId("mi2")+
                ") { return ("+getId("mi1")+"<"+getId("mi2")+")?-1:0; }\n");
      decrLevel();
      cb.append("}\n");
    }
  }
  
  /** Creates a new instance of JMap */
  public JMap(JType t1, JType t2) {
    setJavaType(new JavaMap(t1.getJavaType(), t2.getJavaType()));
    setCppType(new CppCompType(" ::std::map<"+t1.getCppType().getType()+","+
                               t2.getCppType().getType()+">"));
    setCType(new CType());
    keyType = t1;
    valueType = t2;
  }
  
  String getSignature() {
    return "{" + keyType.getSignature() + valueType.getSignature() +"}";
  }
}
