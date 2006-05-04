/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
 * @author milindb
 */
public abstract class JType {
    
    private String mCppName;
    private String mJavaName;
    private String mMethodSuffix;
    private String mWrapper;
    private String mUnwrapMethod;
    
    /**
     * Creates a new instance of JType
     */
    public JType(String cppname, String javaname, String suffix, String wrapper, String unwrap) {
        mCppName = cppname;
        mJavaName = javaname;
        mMethodSuffix = suffix;
        mWrapper = wrapper;
        mUnwrapMethod = unwrap;
    }
    
    abstract public String getSignature();
    
    public String genCppDecl(String fname) {
        return "  "+mCppName+" m"+fname+";\n"; 
    }
    
    public String genJavaDecl (String fname) {
        return "  private "+mJavaName+" m"+fname+";\n";
    }
    
    public String genJavaConstructorParam (int fIdx) {
        return "        "+mJavaName+" m"+fIdx;
    }
    
    public String genCppGetSet(String fname, int fIdx) {
        String getFunc = "  virtual "+mCppName+" get"+fname+"() const {\n";
        getFunc += "    return m"+fname+";\n";
        getFunc += "  }\n";
        String setFunc = "  virtual void set"+fname+"("+mCppName+" m_) {\n";
        setFunc += "    m"+fname+"=m_; bs_.set("+fIdx+");\n";
        setFunc += "  }\n";
        return getFunc+setFunc;
    }
    
    public String genJavaGetSet(String fname, int fIdx) {
        String getFunc = "  public "+mJavaName+" get"+fname+"() {\n";
        getFunc += "    return m"+fname+";\n";
        getFunc += "  }\n";
        String setFunc = "  public void set"+fname+"("+mJavaName+" m_) {\n";
        setFunc += "    m"+fname+"=m_; bs_.set("+fIdx+");\n";
        setFunc += "  }\n";
        return getFunc+setFunc;
    }
    
    public String getCppType() {
        return mCppName;
    }
    
    public String getJavaType() {
        return mJavaName;
    }
   
    public String getJavaWrapperType() {
        return mWrapper;
    }
    
    public String getMethodSuffix() {
        return mMethodSuffix;
    }
    
    public String genJavaWriteMethod(String fname, String tag) {
        return "    a_.write"+mMethodSuffix+"("+fname+",\""+tag+"\");\n";
    }
    
    public String genJavaReadMethod(String fname, String tag) {
        return "    "+fname+"=a_.read"+mMethodSuffix+"(\""+tag+"\");\n";
    }
    
    public String genJavaReadWrapper(String fname, String tag, boolean decl) {
        String ret = "";
        if (decl) {
            ret = "    "+mWrapper+" "+fname+";\n";
        }
        return ret + "    "+fname+"=new "+mWrapper+"(a_.read"+mMethodSuffix+"(\""+tag+"\"));\n";
    }
    
    public String genJavaWriteWrapper(String fname, String tag) {
        return "        a_.write"+mMethodSuffix+"("+fname+"."+mUnwrapMethod+"(),\""+tag+"\");\n";
    }
    
    public String genJavaCompareTo(String fname) {
        return "    ret = ("+fname+" == peer."+fname+")? 0 :(("+fname+"<peer."+fname+")?-1:1);\n";
    }
    
    public String genJavaEquals(String fname, String peer) {
        return "    ret = ("+fname+"=="+peer+");\n";
    }
    
    public String genJavaHashCode(String fname) {
        return "    ret = (int)"+fname+";\n";
    }

    String genJavaConstructorSet(String fname, int fIdx) {
        return "    m"+fname+"=m"+fIdx+"; bs_.set("+fIdx+");\n";
    }
}
