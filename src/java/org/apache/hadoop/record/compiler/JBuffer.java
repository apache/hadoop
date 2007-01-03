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
public class JBuffer extends JCompType {
    
    /** Creates a new instance of JBuffer */
    public JBuffer() {
        super(" ::std::string", "java.io.ByteArrayOutputStream", "Buffer", "java.io.ByteArrayOutputStream");
    }
    
    public String genCppGetSet(String fname, int fIdx) {
        String cgetFunc = "  virtual const "+getCppType()+"& get"+fname+"() const {\n";
        cgetFunc += "    return m"+fname+";\n";
        cgetFunc += "  }\n";
        String getFunc = "  virtual "+getCppType()+"& get"+fname+"() {\n";
        getFunc += "    bs_.set("+fIdx+");return m"+fname+";\n";
        getFunc += "  }\n";
        return cgetFunc + getFunc;
    }
    
    public String getSignature() {
        return "B";
    }
    
    public String genJavaReadWrapper(String fname, String tag, boolean decl) {
        String ret = "";
        if (decl) {
            ret = "    java.io.ByteArrayOutputStream "+fname+";\n";
        }
        return ret + "        "+fname+"=a_.readBuffer(\""+tag+"\");\n";
    }
    
    public String genJavaWriteWrapper(String fname, String tag) {
        return "        a_.writeBuffer("+fname+",\""+tag+"\");\n";
    }
    
    public String genJavaCompareTo(String fname, String other) {
      StringBuffer sb = new StringBuffer();
      sb.append("    {\n");
      sb.append("      byte[] my = "+fname+".toByteArray();\n");
      sb.append("      byte[] ur = "+other+".toByteArray();\n");
      sb.append("      ret = WritableComparator.compareBytes(my,0,my.length,ur,0,ur.length);\n");
      sb.append("    }\n");
      return sb.toString();
    }
    
    public String genJavaCompareToWrapper(String fname, String other) {
      return "    "+genJavaCompareTo(fname, other);
    }
    
    public String genJavaEquals(String fname, String peer) {
        return "    ret = org.apache.hadoop.record.Utils.bufEquals("+fname+","+peer+");\n";
    }
    
    public String genJavaHashCode(String fname) {
        return "    ret = "+fname+".toString().hashCode();\n";
    }
    
    public String genJavaSlurpBytes(String b, String s, String l) {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("           int i = WritableComparator.readVInt("+b+", "+s+");\n");
      sb.append("           int z = WritableUtils.getVIntSize(i);\n");
      sb.append("           "+s+" += z+i; "+l+" -= (z+i);\n");
      sb.append("        }\n");
      return sb.toString();
    }
    
    public String genJavaCompareBytes() {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("           int i1 = WritableComparator.readVInt(b1, s1);\n");
      sb.append("           int i2 = WritableComparator.readVInt(b2, s2);\n");
      sb.append("           int z1 = WritableUtils.getVIntSize(i1);\n");
      sb.append("           int z2 = WritableUtils.getVIntSize(i2);\n");
      sb.append("           s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
      sb.append("           int r1 = WritableComparator.compareBytes(b1,s1,l1,b2,s2,l2);\n");
      sb.append("           if (r1 != 0) { return (r1<0)?-1:0; }\n");
      sb.append("           s1+=i1; s2+=i2; l1-=i1; l1-=i2;\n");
      sb.append("        }\n");
      return sb.toString();
    }
}
