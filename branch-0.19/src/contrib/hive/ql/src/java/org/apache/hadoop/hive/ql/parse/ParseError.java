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

package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.*;

/*
 * SemanticException.java
 *
 * Created on April 1, 2008, 1:20 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

/**
 *
 */
public class ParseError {
  private BaseRecognizer br;
  private RecognitionException re;
  private String[] tokenNames;
  
  ParseError(BaseRecognizer br, RecognitionException re, String[] tokenNames) {
    this.br = br;
    this.re = re;
    this.tokenNames = tokenNames;
    }
  
  BaseRecognizer getBaseRecognizer() {
    return br;
  }

  RecognitionException getRecognitionException() {
    return re;
  }
  
  String[] getTokenNames() {
    return tokenNames;
  }

  String getMessage() {
    return br.getErrorHeader(re) + " " + br.getErrorMessage(re, tokenNames);
  }

}
