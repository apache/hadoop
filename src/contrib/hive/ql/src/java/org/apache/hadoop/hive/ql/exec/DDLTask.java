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

package org.apache.hadoop.hive.ql.exec;

import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.alterTableDesc;
import org.apache.hadoop.hive.ql.plan.createTableDesc;
import org.apache.hadoop.hive.ql.plan.descTableDesc;
import org.apache.hadoop.hive.ql.plan.dropTableDesc;
import org.apache.hadoop.hive.ql.plan.showTablesDesc;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.facebook.thrift.TException;

/**
 * DDLTask implementation
 * 
 **/
public class DDLTask extends Task<DDLWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  static final private Log LOG = LogFactory.getLog("hive.ql.exec.DDLTask");

  transient HiveConf conf;
  static final private int separator  = Utilities.ctrlaCode;
  static final private int terminator = Utilities.newLineCode;
  
  public void initialize(HiveConf conf) {
    super.initialize(conf);
    this.conf = conf;
  }

  public int execute() {

    // Create the db
    Hive db;
    FileSystem fs;
    try {
      db = Hive.get(conf);
      fs = FileSystem.get(conf);

      createTableDesc crtTbl = work.getCreateTblDesc();
      if (crtTbl != null) {

        // create the table
        Table tbl = new Table(crtTbl.getTableName());
        tbl.setFields(crtTbl.getCols());
        StorageDescriptor tblStorDesc = tbl.getTTable().getSd();
        if (crtTbl.getBucketCols() != null)
          tblStorDesc.setBucketCols(crtTbl.getBucketCols());
        if (crtTbl.getSortCols() != null)
          tbl.setSortCols(crtTbl.getSortCols());
        if (crtTbl.getPartCols() != null)
          tbl.setPartCols(crtTbl.getPartCols());
        if (crtTbl.getNumBuckets() != -1)
          tblStorDesc.setNumBuckets(crtTbl.getNumBuckets());
        if (crtTbl.getFieldDelim() != null)
          tbl.setSerdeParam(Constants.FIELD_DELIM, crtTbl.getFieldDelim());
        if (crtTbl.getCollItemDelim() != null)
          tbl.setSerdeParam(Constants.COLLECTION_DELIM, crtTbl.getCollItemDelim());
        if (crtTbl.getMapKeyDelim() != null)
          tbl.setSerdeParam(Constants.MAPKEY_DELIM, crtTbl.getMapKeyDelim());
        if (crtTbl.getLineDelim() != null)
          tbl.setSerdeParam(Constants.LINE_DELIM, crtTbl.getLineDelim());
        if (crtTbl.getComment() != null)
          tbl.setProperty("comment", crtTbl.getComment());
        if (crtTbl.getLocation() != null)
          tblStorDesc.setLocation(crtTbl.getLocation());

        if (crtTbl.isSequenceFile()) {
          tbl.setInputFormatClass(SequenceFileInputFormat.class);
          tbl.setOutputFormatClass(SequenceFileOutputFormat.class);
        }
        else {
          tbl.setOutputFormatClass(IgnoreKeyTextOutputFormat.class);
          tbl.setInputFormatClass(TextInputFormat.class);
        } 

        if (crtTbl.isExternal())
          tbl.setProperty("EXTERNAL", "TRUE");

        // If the sorted columns is a superset of bucketed columns, store this fact. It can be later used to
        // optimize some group-by queries. Note that, the order does not matter as long as it in the first
        // 'n' columns where 'n' is the length of the bucketed columns.
        if ((tbl.getBucketCols() != null) && (tbl.getSortCols() != null))
        {
          List<String> bucketCols = tbl.getBucketCols();
          List<Order> sortCols = tbl.getSortCols();

          if (sortCols.size() >= bucketCols.size())
          {
            boolean found = true;

            Iterator<String> iterBucketCols = bucketCols.iterator();
            while (iterBucketCols.hasNext())
            {
              String bucketCol = iterBucketCols.next();
              boolean colFound = false;
              for (int i = 0; i < bucketCols.size(); i++)
              {
                if (bucketCol.equals(sortCols.get(i).getCol())) {
                  colFound = true;
                  break;
                }
              }
              if (colFound == false)
              {
                found = false;
                break;
              }
            }
            if (found)
              tbl.setProperty("SORTBUCKETCOLSPREFIX", "TRUE");
          }
        }

        // create the table
        db.createTable(tbl);
        return 0;
      }

      dropTableDesc dropTbl = work.getDropTblDesc();
      if (dropTbl != null) {
        // drop the table
        db.dropTable(dropTbl.getTableName());
        return 0;
      }

      alterTableDesc alterTbl = work.getAlterTblDesc();
      if (alterTbl != null) {
        // alter the table
        Table tbl = db.getTable(alterTbl.getOldName());
         if (alterTbl.getOp() == alterTableDesc.alterTableTypes.RENAME)
           tbl.getTTable().setTableName(alterTbl.getNewName());
         else
           tbl.getTTable().getSd().setCols(alterTbl.getNewCols());

        try {
          db.alterTable(alterTbl.getOldName(), tbl);
        } catch (InvalidOperationException e) {
          LOG.info("alter table: " + StringUtils.stringifyException(e));
          return 1;
        } catch (MetaException e) {
          return 1;
        } catch (TException e) {
          return 1;
        }
        return 0;
      }

      descTableDesc descTbl = work.getDescTblDesc();
      if (descTbl != null) {
        boolean found = true;

        try {
          // describe the table - populate the output stream
          Table tbl = db.getTable(descTbl.getTableName());
          
          LOG.info("DDLTask: got data for " +  tbl.getName());
          
          // write the results in the file
          DataOutput os = (DataOutput)fs.create(descTbl.getResFile());
          List<FieldSchema> cols = tbl.getCols();
          Iterator<FieldSchema> iterCols = cols.iterator();
          boolean firstCol = true;
          while (iterCols.hasNext())
          {
            if (!firstCol)
              os.write(terminator);
            FieldSchema col = iterCols.next();
            os.write(col.getName().getBytes("UTF-8"));
            os.write(separator);
            os.write(col.getType().getBytes("UTF-8"));
            if (col.getComment() != null)
            {
              os.write(separator);
              os.write(col.getComment().getBytes("UTF-8"));
            }
            firstCol = false;
          }

          // also return the partitioning columns
          List<FieldSchema> partCols = tbl.getPartCols();
          Iterator<FieldSchema> iterPartCols = partCols.iterator();
          while (iterPartCols.hasNext())
          {
            os.write(terminator);
            FieldSchema col = iterPartCols.next();
            os.write(col.getName().getBytes("UTF-8"));
            os.write(separator);
            os.write(col.getType().getBytes("UTF-8"));
            if (col.getComment() != null)
            {
              os.write(separator);
              os.write(col.getComment().getBytes("UTF-8"));
            }
          }
          LOG.info("DDLTask: written data for " +  tbl.getName());
          ((FSDataOutputStream)os).close();
          
        } catch (FileNotFoundException e) {
          LOG.info("describe table: " + StringUtils.stringifyException(e));
          return 1;
        }
        catch (InvalidTableException e) {
          found = false;
        }
        catch (IOException e) {
          LOG.info("describe table: " + StringUtils.stringifyException(e));
          return 1;
        }

        if (!found)
        {
          try {
            DataOutput outStream = (DataOutput)fs.open(descTbl.getResFile());
            String errMsg = "Table " + descTbl.getTableName() + " does not exist";
            outStream.write(errMsg.getBytes("UTF-8"));
            ((FSDataOutputStream)outStream).close();
          } catch (FileNotFoundException e) {
            LOG.info("describe table: " + StringUtils.stringifyException(e));
            return 1;
          }
          catch (IOException e) {
            LOG.info("describe table: " + StringUtils.stringifyException(e));
            return 1;
          }
        }
        return 0;
      }

      showTablesDesc showTbls = work.getShowTblsDesc();
      if (showTbls != null) {
        // get the tables for the desired pattenn - populate the output stream
        List<String> tbls = null;
        if (showTbls.getPattern() != null)
        {
          LOG.info("pattern: " + showTbls.getPattern());
          tbls = db.getTablesByPattern(showTbls.getPattern());
          LOG.info("results : " + tbls.size());
        }
        else
          tbls = db.getAllTables();
        
        // write the results in the file
        try {
          DataOutput outStream = (DataOutput)fs.create(showTbls.getResFile());
          SortedSet<String> sortedTbls = new TreeSet<String>(tbls);
          Iterator<String> iterTbls = sortedTbls.iterator();
          boolean firstCol = true;
          while (iterTbls.hasNext())
          {
            if (!firstCol)
              outStream.write(separator);
            outStream.write(iterTbls.next().getBytes("UTF-8"));
            firstCol = false;
          }
          ((FSDataOutputStream)outStream).close();
        } catch (FileNotFoundException e) {
          LOG.info("show table: " + StringUtils.stringifyException(e));
          return 1;
        } catch (IOException e) {
          LOG.info("show table: " + StringUtils.stringifyException(e));
          return 1;
        }
        return 0;
      }

    } catch (HiveException e) {
      console.printError("FAILED: Error in metadata: " + e.getMessage(), "\n" + StringUtils.stringifyException(e));
      LOG.debug(StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      console.printError("Failed with exception " +   e.getMessage(), "\n" + StringUtils.stringifyException(e));
      return (1);
    }
    assert false;
    return 0;
  }
}
