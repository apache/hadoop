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
public class JVector extends JCompType {
    
    static private int level = 0;
    
    static private String getId(String id) { return id+getLevel(); }
    
    static private String getLevel() { return Integer.toString(level); }
    
    static private void incrLevel() { level++; }
    
    static private void decrLevel() { level--; }
    
    private JType mElement;
    
    /** Creates a new instance of JVector */
    public JVector(JType t) {
        super(" ::std::vector<"+t.getCppType()+">", "java.util.ArrayList", "Vector", "java.util.ArrayList");
        mElement = t;
    }
    
    public String getSignature() {
        return "[" + mElement.getSignature() + "]";
    }
    
    public String genJavaCompareTo(String fname, String other) {
      StringBuffer sb = new StringBuffer();
      sb.append("    {\n");
      sb.append("      int "+getId("len1")+" = "+fname+".size();\n");
      sb.append("      int "+getId("len2")+" = "+other+".size();\n");
      sb.append("      for(int "+getId("vidx")+" = 0; "+getId("vidx")+"<"+
          getId("len1")+" && "+getId("vidx")+"<"+getId("len2")+"; "+
          getId("vidx")+"++) {\n");
      sb.append("        "+mElement.getJavaWrapperType()+" "+getId("e1")+
          " = ("+mElement.getJavaWrapperType()+") "+fname+
          ".get("+getId("vidx")+");\n");
      sb.append("        "+mElement.getJavaWrapperType()+" "+getId("e2")+
          " = ("+mElement.getJavaWrapperType()+") "+other+
          ".get("+getId("vidx")+");\n");
      sb.append(mElement.genJavaCompareToWrapper(getId("e1"), getId("e2")));
      sb.append("         if (ret != 0) { return ret; }\n");
      sb.append("      }\n");
      sb.append("      ret = ("+getId("len1")+" - "+getId("len2")+");\n");
      sb.append("    }\n");
      return sb.toString();
    }
    
    public String genJavaCompareToWrapper(String fname, String other) {
      return genJavaCompareTo(fname, other);
    }
    
    public String genJavaReadWrapper(String fname, String tag, boolean decl) {
        StringBuffer ret = new StringBuffer("");
        if (decl) {
            ret.append("      java.util.ArrayList "+fname+";\n");
        }
        ret.append("    {\n");
        incrLevel();
        ret.append("      org.apache.hadoop.record.Index "+getId("vidx")+" = a_.startVector(\""+tag+"\");\n");
        ret.append("      "+fname+"=new java.util.ArrayList();\n");
        ret.append("      for (; !"+getId("vidx")+".done(); "+getId("vidx")+".incr()) {\n");
        ret.append(mElement.genJavaReadWrapper(getId("e"), getId("e"), true));
        ret.append("        "+fname+".add("+getId("e")+");\n");
        ret.append("      }\n");
        ret.append("    a_.endVector(\""+tag+"\");\n");
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
        ret.append("      a_.startVector("+fname+",\""+tag+"\");\n");
        ret.append("      int "+getId("len")+" = "+fname+".size();\n");
        ret.append("      for(int "+getId("vidx")+" = 0; "+getId("vidx")+"<"+getId("len")+"; "+getId("vidx")+"++) {\n");
        ret.append("        "+mElement.getJavaWrapperType()+" "+getId("e")+" = ("+mElement.getJavaWrapperType()+") "+fname+".get("+getId("vidx")+");\n");
        ret.append(mElement.genJavaWriteWrapper(getId("e"), getId("e")));
        ret.append("      }\n");
        ret.append("      a_.endVector("+fname+",\""+tag+"\");\n");
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
      sb.append("           int "+getId("vi")+
          " = WritableComparator.readVInt("+b+", "+s+");\n");
      sb.append("           int "+getId("vz")+
          " = WritableUtils.getVIntSize("+getId("vi")+");\n");
      sb.append("           "+s+"+="+getId("vz")+"; "+l+"-="+getId("vz")+";\n");
      sb.append("           for (int "+getId("vidx")+" = 0; "+getId("vidx")+
          " < "+getId("vi")+"; "+getId("vidx")+"++)");
      sb.append(mElement.genJavaSlurpBytes(b,s,l));
      decrLevel();
      sb.append("        }\n");
      return sb.toString();
    }
    
    public String genJavaCompareBytes() {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      incrLevel();
      sb.append("           int "+getId("vi1")+
          " = WritableComparator.readVInt(b1, s1);\n");
      sb.append("           int "+getId("vi2")+
          " = WritableComparator.readVInt(b2, s2);\n");
      sb.append("           int "+getId("vz1")+
          " = WritableUtils.getVIntSize("+getId("vi1")+");\n");
      sb.append("           int "+getId("vz2")+
          " = WritableUtils.getVIntSize("+getId("vi2")+");\n");
      sb.append("           s1+="+getId("vz1")+"; s2+="+getId("vz2")+
          "; l1-="+getId("vz1")+"; l2-="+getId("vz2")+";\n");
      sb.append("           for (int "+getId("vidx")+" = 0; "+getId("vidx")+
          " < "+getId("vi1")+" && "+getId("vidx")+" < "+getId("vi2")+
          "; "+getId("vidx")+"++)");
      sb.append(mElement.genJavaCompareBytes());
      sb.append("           if ("+getId("vi1")+" != "+getId("vi2")+
          ") { return ("+getId("vi1")+"<"+getId("vi2")+")?-1:0; }\n");
      decrLevel();
      sb.append("        }\n");
      return sb.toString();
    }
}
