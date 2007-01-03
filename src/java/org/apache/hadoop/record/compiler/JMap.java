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
 *
 * @author Milind Bhandarkar
 */
public class JMap extends JCompType {
   
    static private int level = 0;
    
    static private String getLevel() { return Integer.toString(level); }
    
    static private void incrLevel() { level++; }
    
    static private void decrLevel() { level--; }
    
    static private String getId(String id) { return id+getLevel(); }
    
    private JType mKey;
    private JType mValue;
    
    /** Creates a new instance of JMap */
    public JMap(JType t1, JType t2) {
        super(" ::std::map<"+t1.getCppType()+","+t2.getCppType()+">",
                "java.util.TreeMap", "Map", "java.util.TreeMap");
        mKey = t1;
        mValue = t2;
    }
    
    public String getSignature() {
        return "{" + mKey.getSignature() + mValue.getSignature() +"}";
    }
    
    public String genJavaCompareTo(String fname, String other) {
      StringBuffer sb = new StringBuffer();
      sb.append("    {\n");
      sb.append("      java.util.Set "+getId("set1")+" = "+fname+".keySet();\n");
      sb.append("      java.util.Set "+getId("set2")+" = "+other+".keySet();\n");
      sb.append("      java.util.Iterator "+getId("miter1")+" = "+
          getId("set1")+".iterator();\n");
      sb.append("      java.util.Iterator "+getId("miter2")+" = "+
          getId("set2")+".iterator();\n");
      sb.append("      for(; "+getId("miter1")+".hasNext() && "+
          getId("miter2")+".hasNext(); ) {\n");
      sb.append("        "+mKey.getJavaWrapperType()+" "+getId("k1")+
          " = ("+mKey.getJavaWrapperType()+") "+getId("miter1")+".next();\n");
      sb.append("        "+mKey.getJavaWrapperType()+" "+getId("k2")+
          " = ("+mKey.getJavaWrapperType()+") "+getId("miter2")+".next();\n");
      sb.append(mKey.genJavaCompareToWrapper(getId("k1"), getId("k2")));
      sb.append("         if (ret != 0) { return ret; }\n");
      sb.append("      }\n");
      sb.append("      ret = ("+getId("set1")+".size() - "+getId("set2")+".size());\n");
      sb.append("    }\n");
      return sb.toString();
    }
    
    public String genJavaCompareToWrapper(String fname, String other) {
      return genJavaCompareTo(fname, other);
    }
    
    public String genJavaReadWrapper(String fname, String tag, boolean decl) {
        StringBuffer ret = new StringBuffer("");
        if (decl) {
            ret.append("    java.util.TreeMap "+fname+";\n");
        }
        ret.append("    {\n");
        incrLevel();
        ret.append("      org.apache.hadoop.record.Index "+getId("midx")+" = a_.startMap(\""+tag+"\");\n");
        ret.append("      "+fname+"=new java.util.TreeMap();\n");
        ret.append("      for (; !"+getId("midx")+".done(); "+getId("midx")+".incr()) {\n");
        ret.append(mKey.genJavaReadWrapper(getId("k"),getId("k"),true));
        ret.append(mValue.genJavaReadWrapper(getId("v"),getId("v"),true));
        ret.append("        "+fname+".put("+getId("k")+","+getId("v")+");\n");
        ret.append("      }\n");
        ret.append("    a_.endMap(\""+tag+"\");\n");
        decrLevel();
        ret.append("    }\n");
        return ret.toString();
    }
    
    public String genJavaReadMethod(String fname, String tag) {
        return genJavaReadWrapper(fname, tag, false);
    }
    
    public String genJavaWriteWrapper(String fname, String tag) {
        StringBuffer ret = new StringBuffer("    {\n");
        incrLevel();
        ret.append("      a_.startMap("+fname+",\""+tag+"\");\n");
        ret.append("      java.util.Set "+getId("es")+" = "+fname+".entrySet();\n");
        ret.append("      for(java.util.Iterator "+getId("midx")+" = "+getId("es")+".iterator(); "+getId("midx")+".hasNext(); ) {\n");
        ret.append("        java.util.Map.Entry "+getId("me")+" = (java.util.Map.Entry) "+getId("midx")+".next();\n");
        ret.append("        "+mKey.getJavaWrapperType()+" "+getId("k")+" = ("+mKey.getJavaWrapperType()+") "+getId("me")+".getKey();\n");
        ret.append("        "+mValue.getJavaWrapperType()+" "+getId("v")+" = ("+mValue.getJavaWrapperType()+") "+getId("me")+".getValue();\n");
        ret.append(mKey.genJavaWriteWrapper(getId("k"),getId("k")));
        ret.append(mValue.genJavaWriteWrapper(getId("v"),getId("v")));
        ret.append("      }\n");
        ret.append("      a_.endMap("+fname+",\""+tag+"\");\n");
        ret.append("    }\n");
        decrLevel();
        return ret.toString();
    }
    
    public String genJavaWriteMethod(String fname, String tag) {
        return genJavaWriteWrapper(fname, tag);
    }
    
    public String genJavaSlurpBytes(String b, String s, String l) {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      incrLevel();
      sb.append("           int "+getId("mi")+
          " = WritableComparator.readVInt("+b+", "+s+");\n");
      sb.append("           int "+getId("mz")+
          " = WritableUtils.getVIntSize("+getId("mi")+");\n");
      sb.append("           "+s+"+="+getId("mz")+"; "+l+"-="+getId("mz")+";\n");
      sb.append("           for (int "+getId("midx")+" = 0; "+getId("midx")+
          " < "+getId("mi")+"; "+getId("midx")+"++) {");
      sb.append(mKey.genJavaSlurpBytes(b,s,l));
      sb.append(mValue.genJavaSlurpBytes(b,s,l));
      sb.append("           }\n");
      decrLevel();
      sb.append("        }\n");
      return sb.toString();
    }
    
    public String genJavaCompareBytes() {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      incrLevel();
      sb.append("           int "+getId("mi1")+
          " = WritableComparator.readVInt(b1, s1);\n");
      sb.append("           int "+getId("mi2")+
          " = WritableComparator.readVInt(b2, s2);\n");
      sb.append("           int "+getId("mz1")+
          " = WritableUtils.getVIntSize("+getId("mi1")+");\n");
      sb.append("           int "+getId("mz2")+
          " = WritableUtils.getVIntSize("+getId("mi2")+");\n");
      sb.append("           s1+="+getId("mz1")+"; s2+="+getId("mz2")+
          "; l1-="+getId("mz1")+"; l2-="+getId("mz2")+";\n");
      sb.append("           for (int "+getId("midx")+" = 0; "+getId("midx")+
          " < "+getId("mi1")+" && "+getId("midx")+" < "+getId("mi2")+
          "; "+getId("midx")+"++) {");
      sb.append(mKey.genJavaCompareBytes());
      sb.append(mValue.genJavaSlurpBytes("b1", "s1", "l1"));
      sb.append(mValue.genJavaSlurpBytes("b2", "s2", "l2"));
      sb.append("           }\n");
      sb.append("           if ("+getId("mi1")+" != "+getId("mi2")+
          ") { return ("+getId("mi1")+"<"+getId("mi2")+")?-1:0; }\n");
      decrLevel();
      sb.append("        }\n");
      return sb.toString();
    }
}
