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
    
    public String genJavaCompareTo(String fname) {
        return "";
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
}
