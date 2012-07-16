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

package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * General xml utilities.
 *   
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XMLUtils {
  /**
   * Exception that reflects an invalid XML document.
   */
  static public class InvalidXmlException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public InvalidXmlException(String s) {
      super(s);
    }
  }
  
  /**
   * Add a SAX tag with a string inside.
   *
   * @param contentHandler     the SAX content handler
   * @param tag                the element tag to use  
   * @param value              the string to put inside the tag
   */
  public static void addSaxString(ContentHandler contentHandler,
      String tag, String val) throws SAXException {
    contentHandler.startElement("", "", tag, new AttributesImpl());
    char c[] = val.toString().toCharArray();
    contentHandler.characters(c, 0, c.length);
    contentHandler.endElement("", "", tag);
  }

  /**
   * Represents a bag of key-value pairs encountered during parsing an XML
   * file.
   */
  static public class Stanza {
    private TreeMap<String, LinkedList <Stanza > > subtrees;
    private String value;
    
    public Stanza() {
      subtrees = new TreeMap<String, LinkedList <Stanza > >();
      value = "";
    }
    
    public void setValue(String value) {
      this.value = value;
    }
    
    public String getValue() {
      return this.value;
    }
    
    /** 
     * Discover if a stanza has a given entry.
     *
     * @param name        entry to look for
     * 
     * @return            true if the entry was found
     */
    public boolean hasChildren(String name) {
      return subtrees.containsKey(name);
    }
    
    /** 
     * Pull an entry from a stanza.
     *
     * @param name        entry to look for
     * 
     * @return            the entry
     */
    public List<Stanza> getChildren(String name) throws InvalidXmlException {
      LinkedList <Stanza> children = subtrees.get(name);
      if (children == null) {
        throw new InvalidXmlException("no entry found for " + name);
      }
      return children;
    }
    
    /** 
     * Pull a string entry from a stanza.
     *
     * @param name        entry to look for
     * 
     * @return            the entry
     */
    public String getValue(String name) throws InvalidXmlException {
      if (!subtrees.containsKey(name)) {
        throw new InvalidXmlException("no entry found for " + name);
      }
      LinkedList <Stanza> l = subtrees.get(name);
      if (l.size() != 1) {
        throw new InvalidXmlException("More than one value found for " + name);
      }
      return l.get(0).getValue();
    }
    
    /** 
     * Add an entry to a stanza.
     *
     * @param name        name of the entry to add
     * @param child       the entry to add
     */
    public void addChild(String name, Stanza child) {
      LinkedList<Stanza> l;
      if (subtrees.containsKey(name)) {
        l = subtrees.get(name);
      } else {
        l = new LinkedList<Stanza>();
        subtrees.put(name, l);
      }
      l.add(child);
    }
    
    /** 
     * Convert a stanza to a human-readable string.
     */
    @Override
    public String toString() {
      StringBuilder bld = new StringBuilder();
      bld.append("{");
      if (!value.equals("")) {
        bld.append("\"").append(value).append("\"");
      }
      String prefix = "";
      for (Map.Entry<String, LinkedList <Stanza > > entry :
          subtrees.entrySet()) {
        String key = entry.getKey();
        LinkedList <Stanza > ll = entry.getValue();
        for (Stanza child : ll) {
          bld.append(prefix);
          bld.append("<").append(key).append(">");
          bld.append(child.toString());
          prefix = ", ";
        }
      }
      bld.append("}");
      return bld.toString();
    }
  }
}