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
public class JString extends JCompType {
    
    /** Creates a new instance of JString */
    public JString() {
        super(" ::std::string", "Text", "String", "Text");
    }
    
    public String getSignature() {
        return "s";
    }
    
    public String genJavaReadWrapper(String fname, String tag, boolean decl) {
        String ret = "";
        if (decl) {
            ret = "    Text "+fname+";\n";
        }
        return ret + "        "+fname+"=a_.readString(\""+tag+"\");\n";
    }
    
    public String genJavaWriteWrapper(String fname, String tag) {
        return "        a_.writeString("+fname+",\""+tag+"\");\n";
    }
    
    public String genJavaSlurpBytes(String b, String s, String l) {
      StringBuffer sb = new StringBuffer();
      sb.append("        {\n");
      sb.append("           int i = WritableComparator.readVInt("+b+", "+s+");\n");
      sb.append("           int z = WritableUtils.getVIntSize(i);\n");
      sb.append("           "+s+"+=(z+i); "+l+"-= (z+i);\n");
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
