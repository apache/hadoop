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

package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

/**
 * A Hive Table Partition: is a fundamental storage unit within a Table
 */
public class Partition {

    @SuppressWarnings("nls")
    static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Partition");

    private Table table;
    private org.apache.hadoop.hive.metastore.api.Partition tPartition;
    /**
     * @return the tPartition
     */
    public org.apache.hadoop.hive.metastore.api.Partition getTPartition() {
      return tPartition;
    }

    private LinkedHashMap<String, String> spec;
    
    /**
     * @return
     * @see org.apache.hadoop.hive.metastore.api.Partition#getValues()
     */
    public List<String> getValues() {
      return tPartition.getValues();
    }

    private Path partPath;
    private URI partURI;

    Partition(Table tbl, org.apache.hadoop.hive.metastore.api.Partition tp) throws HiveException {
      this.table = tbl;
      this.tPartition = tp;
      partName = "";
      if(table.isPartitioned()) {
        try {
          partName = Warehouse.makePartName(tbl.getPartCols(), tp.getValues());
        } catch (MetaException e) {
          throw new HiveException("Invalid partition for table " + tbl.getName(), e);
        }
        this.partPath = new Path(table.getPath(), partName);
      } else {
        // We are in the HACK territory. SemanticAnalyzer expects a single partition whose schema
        // is same as the table partition. 
        this.partPath = table.getPath();
      }
      spec = makeSpecFromPath();
      
      URI tmpURI = table.getDataLocation();
      try {
        partURI = new URI(tmpURI.getScheme(), tmpURI.getAuthority(),
                          tmpURI.getPath() + "/" + partName, null, null);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
    
    // This is used when a Partition object is created solely from the hdfs partition directories
    Partition(Table tbl, Path path) throws HiveException {
      this.table = tbl;
      // initialize the tPartition(thrift object) with the data from path and  table
      this.tPartition = new org.apache.hadoop.hive.metastore.api.Partition();
      this.tPartition.setDbName(tbl.getDbName());
      this.tPartition.setTableName(tbl.getName());
      StorageDescriptor sd = tbl.getTTable().getSd();
      StorageDescriptor psd = new StorageDescriptor(
          sd.getCols(), sd.getLocation(), sd.getInputFormat(), sd.getOutputFormat(),
          sd.isCompressed(), sd.getNumBuckets(), sd.getSerdeInfo(), sd.getBucketCols(),
          sd.getSortCols(), new HashMap<String, String>());
      this.tPartition.setSd(psd);
      // change the partition location
      if(table.isPartitioned()) {
        this.partPath = path;
      } else {
        // We are in the HACK territory. SemanticAnalyzer expects a single partition whose schema
        // is same as the table partition. 
        this.partPath = table.getPath();
      }
      spec = makeSpecFromPath();
      psd.setLocation(this.partPath.toString());
      List<String> partVals = new ArrayList<String> ();
      tPartition.setValues(partVals);
      for (FieldSchema field : tbl.getPartCols()) {
        partVals.add(spec.get(field.getName()));
      }
      try {
        this.partName = Warehouse.makePartName(tbl.getPartCols(), partVals);
      } catch (MetaException e) {
        throw new HiveException("Invalid partition key values", e);
      }
    }
    
    static final Pattern pat = Pattern.compile("([^/]+)=([^/]+)");
    private LinkedHashMap<String, String> makeSpecFromPath() throws HiveException {
      // Keep going up the path till it equals the parent
      Path currPath = this.partPath;
      LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
      List<FieldSchema> pcols = this.table.getPartCols();
      for(int i = 0; i < pcols.size(); i++) {
        FieldSchema col =  pcols.get(pcols.size() - i - 1);
        if (currPath == null) {
          throw new HiveException("Out of path components while expecting key: " + col.getName());
        }
        String component = currPath.getName();
        // Check if the component is either of the form k=v
        // or is the first component
        // if neither is true then this is an invalid path
        Matcher m = pat.matcher(component);
        if (m.matches()) {
          String k = m.group(1);
          String v = m.group(2);

          if (!k.equals(col.getName())) {
            throw new HiveException("Key mismatch expected: " + col.getName() + " and got: " + k);
          }
          if (partSpec.containsKey(k)) {
            throw new HiveException("Key " + k + " defined at two levels");
          }

          partSpec.put(k, v);
        }
        else {
          throw new HiveException("Path " + currPath.toString() + " not a valid path");
        }
        currPath = currPath.getParent();
      }
      // reverse the list since we checked the part from leaf dir to table's base dir
      LinkedHashMap<String, String> newSpec = new LinkedHashMap<String, String>();
      for(int i = 0; i < table.getPartCols().size(); i++) {
        FieldSchema  field = table.getPartCols().get(i);
        String val = partSpec.get(field.getName());
        newSpec.put(field.getName(), val);
      }
      return newSpec;
    }

    public URI makePartURI(LinkedHashMap<String, String> spec) throws HiveException {
      StringBuffer suffix = new StringBuffer();
      if (table.getPartCols() != null) {
        for(FieldSchema k: table.getPartCols()) {
          suffix.append(k + "=" + spec.get(k.getName()) + "/");
        }
      }
      URI tmpURI = table.getDataLocation();
      try {
        return new URI(tmpURI.getScheme(), tmpURI.getAuthority(),
            tmpURI.getPath() + suffix.toString(), null, null);
      } catch (URISyntaxException e) {
        throw new HiveException(e);
      }
    }

    public String getName() {
        return partName;
    }

    public Table getTable() {
        return (this.table);
    }

    public Path [] getPath() {
        Path [] ret = new Path [1];
        ret[0] = this.partPath;
        return(ret);
    }

    final public URI getDataLocation() {
      return this.partURI;
    }

    /**
     * The number of buckets is a property of the partition. However - internally we are just
     * storing it as a property of the table as a short term measure.
     */
    public int getBucketCount() {
      return this.table.getNumBuckets();
      /*
      TODO: Keeping this code around for later use when we will support
      sampling on tables which are not created with CLUSTERED INTO clause
 
      // read from table meta data
      int numBuckets = this.table.getNumBuckets();
      if (numBuckets == -1) {
        // table meta data does not have bucket information
        // check if file system has multiple buckets(files) in this partition
        String pathPattern = this.partPath.toString() + "/*";
        try {
          FileSystem fs = FileSystem.get(this.table.getDataLocation(), Hive.get().getConf());
          FileStatus srcs[] = fs.globStatus(new Path(pathPattern));
          numBuckets = srcs.length;
        }
        catch (Exception e) {
          throw new RuntimeException("Cannot get bucket count for table " + this.table.getName(), e);
        }
      }
      return numBuckets;
      */
    }
    
    public List<String> getBucketCols() {
      return this.table.getBucketCols();
    }

    /**
     * mapping from bucket number to bucket path
     */
    //TODO: add test case and clean it up
    @SuppressWarnings("nls")
    public Path getBucketPath(int bucketNum) {
        try {
            FileSystem fs = FileSystem.get(this.table.getDataLocation(), Hive.get().getConf());
            String pathPattern = this.partPath.toString();
            if (getBucketCount() > 0) {
              pathPattern = pathPattern + "/*";
            }
            LOG.info("Path pattern = " + pathPattern);
            FileStatus srcs[] = fs.globStatus(new Path(pathPattern));
            for (FileStatus src: srcs) {
                LOG.info("Got file: " + src.getPath());
            }
            return srcs[bucketNum].getPath();
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot get bucket path for bucket " + bucketNum, e);
        }
        // return new Path(this.partPath, String.format("part-%1$05d", bucketNum));
    }

    /**
     * mapping from a Path to the bucket number if any
     */
    private static Pattern bpattern = Pattern.compile("part-([0-9][0-9][0-9][0-9][0-9])");

    private String partName;
    @SuppressWarnings("nls")
    public static int getBucketNum(Path p) {
        Matcher m = bpattern.matcher(p.getName());
        if(m.find()) {
            String bnum_str = m.group(1);
            try {
                return (Integer.parseInt(bnum_str));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Unexpected error parsing: "+p.getName()+","+bnum_str);
            }
        }
        return 0;
    }


    @SuppressWarnings("nls")
    public Path [] getPath(Sample s) throws HiveException {
        if(s == null) {
            return getPath();
        } else {
            int bcount = this.getBucketCount();
            if(bcount == 0) {
                return getPath();
            }

            Dimension d = s.getSampleDimension();
            if(!d.getDimensionId().equals(this.table.getBucketingDimensionId())) {
                // if the bucket dimension is not the same as the sampling dimension
                // we must scan all the data
                return getPath();
            }

            int scount = s.getSampleFraction();
            ArrayList<Path> ret = new ArrayList<Path> ();

            if(bcount == scount) {
                ret.add(getBucketPath(s.getSampleNum()-1));
            } else if (bcount < scount) {
                if((scount/bcount)*bcount != scount) {
                    throw new HiveException("Sample Count"+scount+" is not a multiple of bucket count " +
                                            bcount + " for table " + this.table.getName());
                }
                // undersampling a bucket
                ret.add(getBucketPath((s.getSampleNum()-1)%bcount));
            } else if (bcount > scount) {
                if((bcount/scount)*scount != bcount) {
                    throw new HiveException("Sample Count"+scount+" is not a divisor of bucket count " +
                                            bcount + " for table " + this.table.getName());
                }
                // sampling multiple buckets
                for(int i=0; i<bcount/scount; i++) {
                    ret.add(getBucketPath(i*scount + (s.getSampleNum()-1)));
                }
            }
            return(ret.toArray(new Path[ret.size()]));
        }
    }

    public LinkedHashMap<String, String> getSpec() {
      return this.spec;
    }

    /**
     * Replaces files in the partition with new data set specified by srcf. Works by moving files
     *
     * @param srcf Files to be moved. Leaf Directories or Globbed File Paths
     */
    @SuppressWarnings("nls")
    protected void replaceFiles(Path srcf) throws HiveException {
      FileSystem fs;
      try {
        fs = FileSystem.get(table.getDataLocation(), Hive.get().getConf());
        Hive.get().replaceFiles(srcf, partPath, fs);
      } catch (IOException e) {
        throw new HiveException("addFiles: filesystem error in check phase", e);
      }
    }

    /**
     * Inserts files specified into the partition. Works by moving files
     *
     * @param srcf Files to be moved. Leaf Directories or Globbed File Paths
     */
    @SuppressWarnings("nls")
    protected void copyFiles(Path srcf) throws HiveException {
      FileSystem fs;
      try {
        fs = FileSystem.get(table.getDataLocation(), Hive.get().getConf());
        Hive.get().copyFiles(srcf, partPath, fs);
      } catch (IOException e) {
        throw new HiveException("addFiles: filesystem error in check phase", e);
      }
    }

    @SuppressWarnings("nls")
    @Override
    public String toString() { 
      String pn = "Invalid Partition"; 
      try {
        pn = Warehouse.makePartName(spec);
      } catch (MetaException e) {
        // ignore as we most probably in an exception path already otherwise this error wouldn't occur
      }
      return table.toString() + "(" + pn + ")";
    }
}
