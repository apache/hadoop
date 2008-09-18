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

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import org.antlr.runtime.tree.CommonTree;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.alterTableDesc;
import org.apache.hadoop.hive.ql.plan.createTableDesc;
import org.apache.hadoop.hive.ql.plan.descTableDesc;
import org.apache.hadoop.hive.ql.plan.dropTableDesc;
import org.apache.hadoop.hive.ql.plan.showTablesDesc;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.*;

public class DDLSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final Log LOG = LogFactory.getLog("hive.ql.parse.DDLSemanticAnalyzer");
  public static final Map<Integer, String> TokenToTypeName = new HashMap<Integer, String>();
  static {
    TokenToTypeName.put(HiveParser.TOK_TINYINT, Constants.TINYINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_INT, Constants.INT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_BIGINT, Constants.BIGINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_FLOAT, Constants.FLOAT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DOUBLE, Constants.DOUBLE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_STRING, Constants.STRING_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATE, Constants.DATE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATETIME, Constants.DATETIME_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_TIMESTAMP, Constants.TIMESTAMP_TYPE_NAME);
  }

  public static String getTypeName(int token) {
    return TokenToTypeName.get(token);
  }

  public DDLSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @Override
  public void analyze(CommonTree ast, Context ctx) throws SemanticException {
    this.ctx = ctx;
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE)
       analyzeCreateTable(ast, false);
    else if (ast.getToken().getType() == HiveParser.TOK_CREATEEXTTABLE)
       analyzeCreateTable(ast, true);
    else if (ast.getToken().getType() == HiveParser.TOK_DROPTABLE)
       analyzeDropTable(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_DESCTABLE)
    {
      ctx.setResFile(new File(getTmpFileName()));
      analyzeDescribeTable(ast);
    }
    else if (ast.getToken().getType() == HiveParser.TOK_SHOWTABLES)
    {
      ctx.setResFile(new File(getTmpFileName()));
      analyzeShowTables(ast);
    }
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAME)
      analyzeAlterTableRename(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDCOLS)
      analyzeAlterTableAddCols(ast);
  }

  private void analyzeCreateTable(CommonTree ast, boolean isExt) 
    throws SemanticException {
    String            tableName     = ast.getChild(0).getText();
    CommonTree        colList       = (CommonTree)ast.getChild(1);
    List<FieldSchema> cols          = getColumns(colList);
    List<FieldSchema> partCols      = null;
    List<String>      bucketCols    = null;
    List<String>      sortCols      = null;
    int               numBuckets    = -1;
    String            fieldDelim    = null;
    String            collItemDelim = null;
    String            mapKeyDelim   = null;
    String            lineDelim     = null;
    String            comment       = null;
    boolean           isCompressed  = false;
    String            location      = null;

    LOG.info("Creating table" + tableName);
    int numCh = ast.getChildCount();
    for (int num = 2; num < numCh; num++)
    {
      CommonTree child = (CommonTree)ast.getChild(num);
      switch (child.getToken().getType()) {
        case HiveParser.TOK_TABLECOMMENT:
          comment = child.getChild(0).getText();
          break;
        case HiveParser.TOK_TABLEPARTCOLS:
          partCols = getColumns((CommonTree)child.getChild(0));
          break;
        case HiveParser.TOK_TABLEBUCKETS:
          bucketCols = getColumnNames((CommonTree)child.getChild(0));
          if (child.getChildCount() == 2)
            numBuckets = (Integer.valueOf(child.getChild(1).getText())).intValue();
          else
          {
            sortCols = getColumnNames((CommonTree)child.getChild(1));
            numBuckets = (Integer.valueOf(child.getChild(2).getText())).intValue();
          }
          break;
        case HiveParser.TOK_TABLEROWFORMAT:
          int numChildRowFormat = child.getChildCount();
          for (int numC = 0; numC < numChildRowFormat; numC++)
          {
            CommonTree rowChild = (CommonTree)child.getChild(numC);
            switch (rowChild.getToken().getType()) {
              case HiveParser.TOK_TABLEROWFORMATFIELD:
                fieldDelim = unescapeSQLString(rowChild.getChild(0).getText());
                break;
              case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
                collItemDelim = unescapeSQLString(rowChild.getChild(0).getText());
                break;
              case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
                mapKeyDelim = unescapeSQLString(rowChild.getChild(0).getText());
                break;
              case HiveParser.TOK_TABLEROWFORMATLINES:
                lineDelim = unescapeSQLString(rowChild.getChild(0).getText());
                break;
              default: assert false;
            }
          }
          break;
        case HiveParser.TOK_TBLCOMPRESSED:
          isCompressed = true;
          break;
        case HiveParser.TOK_TABLELOCATION:
          location = unescapeSQLString(child.getChild(0).getText());
          break;
        default: assert false;
      }
    }

    createTableDesc crtTblDesc = 
      new createTableDesc(tableName, isExt, cols, partCols, bucketCols, 
                          sortCols, numBuckets,
                          fieldDelim, collItemDelim, mapKeyDelim, lineDelim,
                          comment, isCompressed, location);

    validateCreateTable(crtTblDesc);
    rootTasks.add(TaskFactory.get(new DDLWork(crtTblDesc), conf));
    
  }

  private void validateCreateTable(createTableDesc crtTblDesc) throws SemanticException {
    // no duplicate column names
    // currently, it is a simple n*n algorithm - this can be optimized later if need be
    // but it should not be a major bottleneck as the number of columns are anyway not so big
    Iterator<FieldSchema> iterCols = crtTblDesc.getCols().iterator();
    List<String> colNames = new ArrayList<String>();
    while (iterCols.hasNext()) {
      String colName = iterCols.next().getName();
      Iterator<String> iter = colNames.iterator();
      while (iter.hasNext()) {
        String oldColName = iter.next();
        if (colName.equalsIgnoreCase(oldColName)) 
          throw new SemanticException(ErrorMsg.DUPLICATE_COLUMN_NAMES.getMsg());
      }
      colNames.add(colName);
    }

    if (crtTblDesc.getBucketCols() != null)
    {    
      // all columns in cluster and sort are valid columns
      Iterator<String> bucketCols = crtTblDesc.getBucketCols().iterator();
      while (bucketCols.hasNext()) {
        String bucketCol = bucketCols.next();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (bucketCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found)
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
      }
    }

    if (crtTblDesc.getSortCols() != null)
    {
      // all columns in cluster and sort are valid columns
      Iterator<String> sortCols = crtTblDesc.getSortCols().iterator();
      while (sortCols.hasNext()) {
        String sortCol = sortCols.next();
        boolean found = false;
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (sortCol.equalsIgnoreCase(colName)) {
            found = true;
            break;
          }
        }
        if (!found)
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg());
      }
    }
    
    if (crtTblDesc.getPartCols() != null)
    {
      // there is no overlap between columns and partitioning columns
      Iterator<FieldSchema> partColsIter = crtTblDesc.getPartCols().iterator();
      while (partColsIter.hasNext()) {
        String partCol = partColsIter.next().getName();
        Iterator<String> colNamesIter = colNames.iterator();
        while (colNamesIter.hasNext()) {
          String colName = colNamesIter.next();
          if (partCol.equalsIgnoreCase(colName)) 
            throw new SemanticException(ErrorMsg.COLUMN_REPAEATED_IN_PARTITIONING_COLS.getMsg());
        }
      }
    }
  }
  
  private void analyzeDropTable(CommonTree ast) 
    throws SemanticException {
    String tableName = ast.getChild(0).getText();    
    dropTableDesc dropTblDesc = new dropTableDesc(tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(dropTblDesc), conf));
  }

  private List<FieldSchema> getColumns(CommonTree ast)
  {
    List<FieldSchema> colList = new ArrayList<FieldSchema>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      FieldSchema col = new FieldSchema();
      CommonTree child = (CommonTree)ast.getChild(i);
      col.setName(child.getChild(0).getText());
      CommonTree typeChild = (CommonTree)(child.getChild(1));
      if (typeChild.getToken().getType() == HiveParser.TOK_LIST)
      {
        CommonTree typName = (CommonTree)typeChild.getChild(0);
        col.setType(MetaStoreUtils.getListType(getTypeName(typName.getToken().getType())));
      }
      else if (typeChild.getToken().getType() == HiveParser.TOK_MAP)
      {
        CommonTree ltypName = (CommonTree)typeChild.getChild(0);
        CommonTree rtypName = (CommonTree)typeChild.getChild(1);
        col.setType(MetaStoreUtils.getMapType(getTypeName(ltypName.getToken().getType()), getTypeName(rtypName.getToken().getType())));
      }
      else                                // primitive type
        col.setType(getTypeName(typeChild.getToken().getType()));
        
      if (child.getChildCount() == 3)
        col.setComment(child.getChild(2).getText());
      colList.add(col);
    }
    return colList;
  }
  
  private List<String> getColumnNames(CommonTree ast)
  {
    List<String> colList = new ArrayList<String>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      CommonTree child = (CommonTree)ast.getChild(i);
      colList.add(child.getChild(0).getText());
    }
    return colList;
  }
  
  private void analyzeDescribeTable(CommonTree ast) 
  throws SemanticException {
    String tableName = ast.getChild(0).getText();    
    descTableDesc descTblDesc = new descTableDesc(ctx.getResFile(), tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(descTblDesc), conf));
    LOG.info("analyzeDescribeTable done");
  }
  
  private void analyzeShowTables(CommonTree ast) 
  throws SemanticException {
    showTablesDesc showTblsDesc;
    if (ast.getChildCount() == 1)
    {
      String tableNames = unescapeSQLString(ast.getChild(0).getText());    
      showTblsDesc = new showTablesDesc(ctx.getResFile(), tableNames);
    }
    else
      showTblsDesc = new showTablesDesc(ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(showTblsDesc), conf));
  }

  private void analyzeAlterTableRename(CommonTree ast) 
  throws SemanticException {
    alterTableDesc alterTblDesc = new alterTableDesc(ast.getChild(0).getText(), ast.getChild(1).getText());
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeAlterTableAddCols(CommonTree ast) 
  throws SemanticException {
    String tblName = ast.getChild(0).getText();
    List<FieldSchema> newCols = getColumns((CommonTree)ast.getChild(1));
    Table tbl;
    try {
      tbl = db.getTable(tblName);
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage()); 
    }
    List<FieldSchema> oldCols = tbl.getCols();
    
    // make sure the columns does not already exist
    Iterator<FieldSchema> iterNewCols = newCols.iterator();
    while (iterNewCols.hasNext()) {
      FieldSchema newCol = iterNewCols.next();
      String newColName  = newCol.getName();
      Iterator<FieldSchema> iterOldCols = oldCols.iterator();
      while (iterOldCols.hasNext()) {
        String oldColName = iterOldCols.next().getName();
        if (oldColName.equalsIgnoreCase(newColName)) 
          throw new SemanticException(ErrorMsg.DUPLICATE_COLUMN_NAMES.getMsg());
      }
      oldCols.add(newCol);
    }

    alterTableDesc alterTblDesc = new alterTableDesc(tblName, oldCols);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

}
