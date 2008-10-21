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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.alterTableDesc;
import org.apache.hadoop.hive.ql.plan.createTableDesc;
import org.apache.hadoop.hive.ql.plan.descTableDesc;
import org.apache.hadoop.hive.ql.plan.dropTableDesc;
import org.apache.hadoop.hive.ql.plan.showPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.showTablesDesc;
import org.apache.hadoop.hive.ql.plan.alterTableDesc.alterTableTypes;
import org.apache.hadoop.hive.serde.Constants;

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
  public void analyzeInternal(CommonTree ast, Context ctx) throws SemanticException {
    this.ctx = ctx;
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE)
       analyzeCreateTable(ast, false);
    else if (ast.getToken().getType() == HiveParser.TOK_CREATEEXTTABLE)
       analyzeCreateTable(ast, true);
    else if (ast.getToken().getType() == HiveParser.TOK_DROPTABLE)
       analyzeDropTable(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_DESCTABLE)
    {
      ctx.setResFile(new Path(getTmpFileName()));
      analyzeDescribeTable(ast);
    }
    else if (ast.getToken().getType() == HiveParser.TOK_SHOWTABLES)
    {
      ctx.setResFile(new Path(getTmpFileName()));
      analyzeShowTables(ast);
    }
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAME)
      analyzeAlterTableRename(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDCOLS)
      analyzeAlterTableModifyCols(ast, alterTableTypes.ADDCOLS);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_REPLACECOLS)
      analyzeAlterTableModifyCols(ast, alterTableTypes.REPLACECOLS);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_DROPPARTS)
      analyzeAlterTableDropParts(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_SHOWPARTITIONS)
    {
      ctx.setResFile(new Path(getTmpFileName()));
      analyzeShowPartitions(ast);
    }
  }

  private void analyzeCreateTable(CommonTree ast, boolean isExt) 
    throws SemanticException {
    String            tableName     = ast.getChild(0).getText();
    CommonTree        colList       = (CommonTree)ast.getChild(1);
    List<FieldSchema> cols          = getColumns(colList);
    List<FieldSchema> partCols      = null;
    List<String>      bucketCols    = null;
    List<Order>       sortCols      = null;
    int               numBuckets    = -1;
    String            fieldDelim    = null;
    String            collItemDelim = null;
    String            mapKeyDelim   = null;
    String            lineDelim     = null;
    String            comment       = null;
    boolean           isSequenceFile  = false;
    String            location      = null;
    String            serde         = null;
    Map<String, String> mapProp     = null;

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
            sortCols = getColumnNamesOrder((CommonTree)child.getChild(1));
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
        case HiveParser.TOK_TABLESERIALIZER:
          serde = unescapeSQLString(child.getChild(0).getText());
          if (child.getChildCount() == 2) {
            mapProp = new HashMap<String, String>();
            CommonTree prop = (CommonTree)((CommonTree)child.getChild(1)).getChild(0);
            for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
              String key = unescapeSQLString(prop.getChild(propChild).getChild(0).getText());
              String value = unescapeSQLString(prop.getChild(propChild).getChild(1).getText());
              mapProp.put(key,value);
            }
          }
          break;
        case HiveParser.TOK_TBLSEQUENCEFILE:
          isSequenceFile = true;
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
                          comment, isSequenceFile, location, serde, mapProp);

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
      Iterator<Order> sortCols = crtTblDesc.getSortCols().iterator();
      while (sortCols.hasNext()) {
        String sortCol = sortCols.next().getCol();
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
      colList.add(child.getText());
    }
    return colList;
  }

  private List<Order> getColumnNamesOrder(CommonTree ast)
  {
    List<Order> colList = new ArrayList<Order>();
    int numCh = ast.getChildCount();
    for (int i = 0; i < numCh; i++) {
      CommonTree child = (CommonTree)ast.getChild(i);
      if (child.getToken().getType() == HiveParser.TOK_TABSORTCOLNAMEASC)
        colList.add(new Order(child.getChild(0).getText(), 1));
      else
        colList.add(new Order(child.getChild(0).getText(), 0));
    }
    return colList;
  }
  
  private void analyzeDescribeTable(CommonTree ast) 
  throws SemanticException {
    Tree table_t = ast.getChild(0);
    String tableName = table_t.getChild(0).getText();
    HashMap<String, String> partSpec = null;
    // get partition metadata if partition specified
    if (table_t.getChildCount() == 2) {
      CommonTree partspec = (CommonTree) table_t.getChild(1);
      partSpec = new LinkedHashMap<String, String>();
      for (int i = 0; i < partspec.getChildCount(); ++i) {
        CommonTree partspec_val = (CommonTree) partspec.getChild(i);
        String val = stripQuotes(partspec_val.getChild(1).getText());
        partSpec.put(partspec_val.getChild(0).getText(), val);
      }
    }
    
    boolean isExt = ast.getChildCount() > 1;
    descTableDesc descTblDesc = new descTableDesc(ctx.getResFile(), tableName, partSpec, isExt);
    rootTasks.add(TaskFactory.get(new DDLWork(descTblDesc), conf));
    LOG.info("analyzeDescribeTable done");
  }
  
  private void analyzeShowPartitions(CommonTree ast) 
  throws SemanticException {
    showPartitionsDesc showPartsDesc;
    String tableName = ast.getChild(0).getText();
    showPartsDesc = new showPartitionsDesc(tableName, ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(showPartsDesc), conf));
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

  private void analyzeAlterTableModifyCols(CommonTree ast, alterTableTypes alterType) 
  throws SemanticException {
    String tblName = ast.getChild(0).getText();
    List<FieldSchema> newCols = getColumns((CommonTree)ast.getChild(1));
    alterTableDesc alterTblDesc = new alterTableDesc(tblName, newCols, alterType);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeAlterTableDropParts(CommonTree ast) throws SemanticException {
    String tblName = null;
    List<HashMap<String, String>> partSpecs = new ArrayList<HashMap<String, String>>();
    int childIndex = 0;
    // get table metadata
    tblName = ast.getChild(0).getText();
    // get partition metadata if partition specified
    for( childIndex = 1; childIndex < ast.getChildCount(); childIndex++) {
      CommonTree partspec = (CommonTree) ast.getChild(childIndex);
      HashMap<String, String> partSpec = new LinkedHashMap<String, String>();
      for (int i = 0; i < partspec.getChildCount(); ++i) {
        CommonTree partspec_val = (CommonTree) partspec.getChild(i);
        String val = stripQuotes(partspec_val.getChild(1).getText());
        partSpec.put(partspec_val.getChild(0).getText(), val);
      }
      partSpecs.add(partSpec);
    }
    dropTableDesc dropTblDesc = new dropTableDesc(tblName, partSpecs);
    rootTasks.add(TaskFactory.get(new DDLWork(dropTblDesc), conf));
  }
}
