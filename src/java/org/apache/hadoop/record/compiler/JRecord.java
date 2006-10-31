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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author Milind Bhandarkar
 */
public class JRecord extends JCompType {

    private String mFQName;
    private String mName;
    private String mModule;
    private ArrayList mFields;
    
    /**
     * Creates a new instance of JRecord
     */
    public JRecord(String name, ArrayList flist) {
        super(name.replaceAll("\\.","::"), name, "Record", name);
        mFQName = name;
        int idx = name.lastIndexOf('.');
        mName = name.substring(idx+1);
        mModule = name.substring(0, idx);
        mFields = flist;
    }
    
    public String getName() {
        return mName;
    }
    
    public String getJavaFQName() {
        return mFQName;
    }
    
    public String getCppFQName() {
        return mFQName.replaceAll("\\.", "::");
    }
    
    public String getJavaPackage() {
        return mModule;
    }
    
    public String getCppNameSpace() {
        return mModule.replaceAll("\\.", "::");
    }
    
    public ArrayList getFields() {
        return mFields;
    }
    
    public String getSignature() {
        StringBuffer sb = new StringBuffer();
        sb.append("L").append(mName).append("(");
        for (Iterator i = mFields.iterator(); i.hasNext();) {
            String s = ((JField) i.next()).getSignature();
            sb.append(s);
        }
        sb.append(")");
        return sb.toString();
    }
    
    public String genCppDecl(String fname) {
        return "  "+mName+" "+fname+";\n";
    }
    
    public String genJavaReadMethod(String fname, String tag) {
        return genJavaReadWrapper(fname, tag, false);
    }
    
    public String genJavaReadWrapper(String fname, String tag, boolean decl) {
        StringBuffer ret = new StringBuffer("");
        if (decl) {
            ret.append("    "+getJavaFQName()+" "+fname+";\n");
        }
        ret.append("    "+fname+"= new "+getJavaFQName()+"();\n");
        ret.append("    a_.readRecord("+fname+",\""+tag+"\");\n");
        return ret.toString();
    }
    
    public String genJavaWriteWrapper(String fname, String tag) {
        return "    a_.writeRecord("+fname+",\""+tag+"\");\n";
    }
    
    public void genCppCode(FileWriter hh, FileWriter cc)
        throws IOException {
        String[] ns = getCppNameSpace().split("::");
        for (int i = 0; i < ns.length; i++) {
            hh.write("namespace "+ns[i]+" {\n");
        }
        
        hh.write("class "+getName()+" : public ::hadoop::Record {\n");
        hh.write("private:\n");
        
        for (Iterator i = mFields.iterator(); i.hasNext();) {
            JField jf = (JField) i.next();
            hh.write(jf.genCppDecl());
        }
        hh.write("  mutable std::bitset<"+mFields.size()+"> bs_;\n");
        hh.write("public:\n");
        hh.write("  virtual void serialize(::hadoop::OArchive& a_, const char* tag) const;\n");
        hh.write("  virtual void deserialize(::hadoop::IArchive& a_, const char* tag);\n");
        hh.write("  virtual const ::std::string& type() const;\n");
        hh.write("  virtual const ::std::string& signature() const;\n");
        hh.write("  virtual bool validate() const;\n");
        hh.write("  virtual bool operator<(const "+getName()+"& peer_) const;\n");
        hh.write("  virtual bool operator==(const "+getName()+"& peer_) const;\n");
        hh.write("  virtual ~"+getName()+"() {};\n");
        int fIdx = 0;
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            hh.write(jf.genCppGetSet(fIdx));
        }
        hh.write("}; // end record "+getName()+"\n");
        for (int i=ns.length-1; i>=0; i--) {
            hh.write("} // end namespace "+ns[i]+"\n");
        }
        cc.write("void "+getCppFQName()+"::serialize(::hadoop::OArchive& a_, const char* tag) const {\n");
        cc.write("  if (!validate()) throw new ::hadoop::IOException(\"All fields not set.\");\n");
        cc.write("  a_.startRecord(*this,tag);\n");
        fIdx = 0;
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            String name = jf.getName();
            if (jf.getType() instanceof JBuffer) {
                cc.write("  a_.serialize("+name+","+name+".length(),\""+jf.getTag()+"\");\n");
            } else {
                cc.write("  a_.serialize("+name+",\""+jf.getTag()+"\");\n");
            }
            cc.write("  bs_.reset("+fIdx+");\n");
        }
        cc.write("  a_.endRecord(*this,tag);\n");
        cc.write("  return;\n");
        cc.write("}\n");
        
        cc.write("void "+getCppFQName()+"::deserialize(::hadoop::IArchive& a_, const char* tag) {\n");
        cc.write("  a_.startRecord(*this,tag);\n");
        fIdx = 0;
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            String name = jf.getName();
            if (jf.getType() instanceof JBuffer) {
                cc.write("  { size_t len=0; a_.deserialize("+name+",len,\""+jf.getTag()+"\");}\n");
            } else {
                cc.write("  a_.deserialize("+name+",\""+jf.getTag()+"\");\n");
            }
            cc.write("  bs_.set("+fIdx+");\n");
        }
        cc.write("  a_.endRecord(*this,tag);\n");
        cc.write("  return;\n");
        cc.write("}\n");
        
        cc.write("bool "+getCppFQName()+"::validate() const {\n");
        cc.write("  if (bs_.size() != bs_.count()) return false;\n");
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            JType type = jf.getType();
            if (type instanceof JRecord) {
                cc.write("  if (!"+jf.getName()+".validate()) return false;\n");
            }
        }
        cc.write("  return true;\n");
        cc.write("}\n");
        
        cc.write("bool "+getCppFQName()+"::operator< (const "+getCppFQName()+"& peer_) const {\n");
        cc.write("  return (1\n");
        for (Iterator i = mFields.iterator(); i.hasNext();) {
            JField jf = (JField) i.next();
            String name = jf.getName();
            cc.write("    && ("+name+" < peer_."+name+")\n");
        }
        cc.write("  );\n");
        cc.write("}\n");
        
        cc.write("bool "+getCppFQName()+"::operator== (const "+getCppFQName()+"& peer_) const {\n");
        cc.write("  return (1\n");
        for (Iterator i = mFields.iterator(); i.hasNext();) {
            JField jf = (JField) i.next();
            String name = jf.getName();
            cc.write("    && ("+name+" == peer_."+name+")\n");
        }
        cc.write("  );\n");
        cc.write("}\n");
        
        cc.write("const ::std::string&"+getCppFQName()+"::type() const {\n");
        cc.write("  static const ::std::string type_(\""+mName+"\");\n");
        cc.write("  return type_;\n");
        cc.write("}\n");
        
        cc.write("const ::std::string&"+getCppFQName()+"::signature() const {\n");
        cc.write("  static const ::std::string sig_(\""+getSignature()+"\");\n");
        cc.write("  return sig_;\n");
        cc.write("}\n");
        
    }
    
    public void genJavaCode() throws IOException {
        String pkg = getJavaPackage();
        String pkgpath = pkg.replaceAll("\\.", "/");
        File pkgdir = new File(pkgpath);
        if (!pkgdir.exists()) {
            // create the pkg directory
            boolean ret = pkgdir.mkdirs();
            if (!ret) {
                System.out.println("Cannnot create directory: "+pkgpath);
                System.exit(1);
            }
        } else if (!pkgdir.isDirectory()) {
            // not a directory
            System.out.println(pkgpath+" is not a directory.");
            System.exit(1);
        }
        File jfile = new File(pkgdir, getName()+".java");
        FileWriter jj = new FileWriter(jfile);
        jj.write("// File generated by hadoop record compiler. Do not edit.\n");
        jj.write("package "+getJavaPackage()+";\n\n");
        jj.write("import org.apache.hadoop.io.Text;\n\n");
        jj.write("public class "+getName()+" implements org.apache.hadoop.record.Record, org.apache.hadoop.io.WritableComparable {\n");
        for (Iterator i = mFields.iterator(); i.hasNext();) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaDecl());
        }
        jj.write("  private java.util.BitSet bs_;\n");
        jj.write("  public "+getName()+"() {\n");
        jj.write("    bs_ = new java.util.BitSet("+(mFields.size()+1)+");\n");
        jj.write("    bs_.set("+mFields.size()+");\n");
        jj.write("  }\n");
        
        jj.write("  public "+getName()+"(\n");
        int fIdx = 0;
        int fLen = mFields.size();
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaConstructorParam(fIdx));
            jj.write((fLen-1 == fIdx)?"":",\n");
        }
        jj.write(") {\n");
        jj.write("    bs_ = new java.util.BitSet("+(mFields.size()+1)+");\n");
        jj.write("    bs_.set("+mFields.size()+");\n");
        fIdx = 0;
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaConstructorSet(fIdx));
        }
        jj.write("  }\n");
        fIdx = 0;
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaGetSet(fIdx));
        }
        jj.write("  public void serialize(org.apache.hadoop.record.OutputArchive a_, String tag) throws java.io.IOException {\n");
        jj.write("    if (!validate()) throw new java.io.IOException(\"All fields not set:\");\n");
        jj.write("    a_.startRecord(this,tag);\n");
        fIdx = 0;
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaWriteMethodName());
            jj.write("    bs_.clear("+fIdx+");\n");
        }
        jj.write("    a_.endRecord(this,tag);\n");
        jj.write("  }\n");
        
        jj.write("  public void deserialize(org.apache.hadoop.record.InputArchive a_, String tag) throws java.io.IOException {\n");
        jj.write("    a_.startRecord(tag);\n");
        fIdx = 0;
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaReadMethodName());
            jj.write("    bs_.set("+fIdx+");\n");
        }
        jj.write("    a_.endRecord(tag);\n");
        jj.write("}\n");
        
        jj.write("  public String toString() {\n");
        jj.write("    try {\n");
        jj.write("      java.io.ByteArrayOutputStream s =\n");
        jj.write("        new java.io.ByteArrayOutputStream();\n");
        jj.write("      org.apache.hadoop.record.CsvOutputArchive a_ = \n");
        jj.write("        new org.apache.hadoop.record.CsvOutputArchive(s);\n");
        jj.write("      a_.startRecord(this,\"\");\n");
        fIdx = 0;
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaWriteMethodName());
        }
        jj.write("      a_.endRecord(this,\"\");\n");
        jj.write("      return new String(s.toByteArray(), \"UTF-8\");\n");
        jj.write("    } catch (Throwable ex) {\n");
        jj.write("      ex.printStackTrace();\n");
        jj.write("    }\n");
        jj.write("    return \"ERROR\";\n");
        jj.write("  }\n");
        
        jj.write("  public void write(java.io.DataOutput out) throws java.io.IOException {\n");
        jj.write("    org.apache.hadoop.record.BinaryOutputArchive archive = new org.apache.hadoop.record.BinaryOutputArchive(out);\n");
        jj.write("    serialize(archive, \"\");\n");
        jj.write("  }\n");
        
        jj.write("  public void readFields(java.io.DataInput in) throws java.io.IOException {\n");
        jj.write("    org.apache.hadoop.record.BinaryInputArchive archive = new org.apache.hadoop.record.BinaryInputArchive(in);\n");
        jj.write("    deserialize(archive, \"\");\n");
        jj.write("  }\n");
        
        jj.write("  public boolean validate() {\n");
        jj.write("    if (bs_.cardinality() != bs_.length()) return false;\n");
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            JType type = jf.getType();
            if (type instanceof JRecord) {
                jj.write("    if (!"+jf.getName()+".validate()) return false;\n");
            }
        }
        jj.write("    return true;\n");
        jj.write("}\n");
        
        jj.write("  public int compareTo (Object peer_) throws ClassCastException {\n");
        jj.write("    if (!(peer_ instanceof "+getName()+")) {\n");
        jj.write("      throw new ClassCastException(\"Comparing different types of records.\");\n");
        jj.write("    }\n");
        jj.write("    "+getName()+" peer = ("+getName()+") peer_;\n");
        jj.write("    int ret = 0;\n");
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaCompareTo());
            jj.write("    if (ret != 0) return ret;\n");
        }
        jj.write("     return ret;\n");
        jj.write("  }\n");
        
        jj.write("  public boolean equals(Object peer_) {\n");
        jj.write("    if (!(peer_ instanceof "+getName()+")) {\n");
        jj.write("      return false;\n");
        jj.write("    }\n");
        jj.write("    if (peer_ == this) {\n");
        jj.write("      return true;\n");
        jj.write("    }\n");
        jj.write("    "+getName()+" peer = ("+getName()+") peer_;\n");
        jj.write("    boolean ret = false;\n");
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaEquals());
            jj.write("    if (!ret) return ret;\n");
        }
        jj.write("     return ret;\n");
        jj.write("  }\n");
        
        jj.write("  public int hashCode() {\n");
        jj.write("    int result = 17;\n");
        jj.write("    int ret;\n");
        for (Iterator i = mFields.iterator(); i.hasNext(); fIdx++) {
            JField jf = (JField) i.next();
            jj.write(jf.genJavaHashCode());
            jj.write("    result = 37*result + ret;\n");
        }
        jj.write("    return result;\n");
        jj.write("  }\n");
        jj.write("  public static String signature() {\n");
        jj.write("    return \""+getSignature()+"\";\n");
        jj.write("  }\n");
        
        jj.write("}\n");
        
        jj.close();
    }
}
