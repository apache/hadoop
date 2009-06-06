package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.QueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;

public class TestWildcardColumnTracker extends HBaseTestCase
implements HConstants {
  private boolean PRINT = false; 
  
  public void testGet_SingleVersion() {
    if(PRINT) {
      System.out.println("SingleVersion");
    }
    byte [] col1 = Bytes.toBytes("col1");
    byte [] col2 = Bytes.toBytes("col2");
    byte [] col3 = Bytes.toBytes("col3");
    byte [] col4 = Bytes.toBytes("col4");
    byte [] col5 = Bytes.toBytes("col5");
    
    //Create tracker
    List<MatchCode> expected = new ArrayList<MatchCode>();
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    int maxVersions = 1;
    
    ColumnTracker exp = new WildcardColumnTracker(maxVersions);
        
    //Create "Scanner"
    List<byte[]> scanner = new ArrayList<byte[]>();
    scanner.add(col1);
    scanner.add(col2);
    scanner.add(col3);
    scanner.add(col4);
    scanner.add(col5);
    
    //Initialize result
    List<MatchCode> result = new ArrayList<MatchCode>(); 
    
    //"Match"
    for(byte [] col : scanner){
      result.add(exp.checkColumn(col, 0, col.length));
    }
    
    assertEquals(expected.size(), result.size());
    for(int i=0; i< expected.size(); i++){
      assertEquals(expected.get(i), result.get(i));
      if(PRINT){
        System.out.println("Expected " +expected.get(i) + ", actual " +
            result.get(i));
      }
    }
  }

  
  public void testGet_MultiVersion() {
    if(PRINT) {
      System.out.println("\nMultiVersion");
    }
    byte [] col1 = Bytes.toBytes("col1");
    byte [] col2 = Bytes.toBytes("col2");
    byte [] col3 = Bytes.toBytes("col3");
    byte [] col4 = Bytes.toBytes("col4");
    byte [] col5 = Bytes.toBytes("col5");
    
    //Create tracker
    List<MatchCode> expected = new ArrayList<MatchCode>();
    int size = 5;
    for(int i=0; i<size; i++){
      expected.add(MatchCode.INCLUDE);
      expected.add(MatchCode.INCLUDE);
      expected.add(MatchCode.SKIP);
    }
    int maxVersions = 2;
    
    ColumnTracker exp = new WildcardColumnTracker(maxVersions);
        
    //Create "Scanner"
    List<byte[]> scanner = new ArrayList<byte[]>();
    scanner.add(col1);
    scanner.add(col1);
    scanner.add(col1);
    scanner.add(col2);
    scanner.add(col2);
    scanner.add(col2);
    scanner.add(col3);
    scanner.add(col3);
    scanner.add(col3);
    scanner.add(col4);
    scanner.add(col4);
    scanner.add(col4);
    scanner.add(col5);
    scanner.add(col5);
    scanner.add(col5);
    
    //Initialize result
    List<MatchCode> result = new ArrayList<MatchCode>(); 
    
    //"Match"
    for(byte [] col : scanner){
      result.add(exp.checkColumn(col, 0, col.length));
    }
    
    assertEquals(expected.size(), result.size());
    for(int i=0; i< expected.size(); i++){
      assertEquals(expected.get(i), result.get(i));
      if(PRINT){
        System.out.println("Expected " +expected.get(i) + ", actual " +
            result.get(i));
      }
    }
  }
  
  public void testUpdate_SameColumns(){
    if(PRINT) {
      System.out.println("\nUpdate_SameColumns");
    }
    byte [] col1 = Bytes.toBytes("col1");
    byte [] col2 = Bytes.toBytes("col2");
    byte [] col3 = Bytes.toBytes("col3");
    byte [] col4 = Bytes.toBytes("col4");
    byte [] col5 = Bytes.toBytes("col5");
    
    //Create tracker
    List<MatchCode> expected = new ArrayList<MatchCode>();
    int size = 10;
    for(int i=0; i<size; i++){
      expected.add(MatchCode.INCLUDE);
    }
    for(int i=0; i<5; i++){
      expected.add(MatchCode.SKIP);
    }
    
    int maxVersions = 2;
    
    ColumnTracker wild = new WildcardColumnTracker(maxVersions);
        
    //Create "Scanner"
    List<byte[]> scanner = new ArrayList<byte[]>();
    scanner.add(col1);
    scanner.add(col2);
    scanner.add(col3);
    scanner.add(col4);
    scanner.add(col5);
    
    //Initialize result
    List<MatchCode> result = new ArrayList<MatchCode>(); 
    
    //"Match"
    for(int i=0; i<3; i++){
      for(byte [] col : scanner){
        result.add(wild.checkColumn(col, 0, col.length));
      }
      wild.update();
    }
    
    assertEquals(expected.size(), result.size());
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), result.get(i));
      if(PRINT){
        System.out.println("Expected " +expected.get(i) + ", actual " +
            result.get(i));
      }
    }
  }
  
  
  public void testUpdate_NewColumns(){
    if(PRINT) {
      System.out.println("\nUpdate_NewColumns");
    }
    byte [] col1 = Bytes.toBytes("col1");
    byte [] col2 = Bytes.toBytes("col2");
    byte [] col3 = Bytes.toBytes("col3");
    byte [] col4 = Bytes.toBytes("col4");
    byte [] col5 = Bytes.toBytes("col5");
    
    byte [] col6 = Bytes.toBytes("col6");
    byte [] col7 = Bytes.toBytes("col7");
    byte [] col8 = Bytes.toBytes("col8");
    byte [] col9 = Bytes.toBytes("col9");
    byte [] col0 = Bytes.toBytes("col0");
    
    //Create tracker
    List<MatchCode> expected = new ArrayList<MatchCode>();
    int size = 10;
    for(int i=0; i<size; i++){
      expected.add(MatchCode.INCLUDE);
    }
    for(int i=0; i<5; i++){
      expected.add(MatchCode.SKIP);
    }
    
    int maxVersions = 1;
    
    ColumnTracker wild = new WildcardColumnTracker(maxVersions);
        
    //Create "Scanner"
    List<byte[]> scanner = new ArrayList<byte[]>();
    scanner.add(col0);
    scanner.add(col1);
    scanner.add(col2);
    scanner.add(col3);
    scanner.add(col4);
    
    //Initialize result
    List<MatchCode> result = new ArrayList<MatchCode>(); 
    
    for(byte [] col : scanner){
      result.add(wild.checkColumn(col, 0, col.length));
    }
    wild.update();

    //Create "Scanner1"
    List<byte[]> scanner1 = new ArrayList<byte[]>();
    scanner1.add(col5);
    scanner1.add(col6);
    scanner1.add(col7);
    scanner1.add(col8);
    scanner1.add(col9);
    for(byte [] col : scanner1){
      result.add(wild.checkColumn(col, 0, col.length));
    }
    wild.update();

    //Scanner again
    for(byte [] col : scanner){
      result.add(wild.checkColumn(col, 0, col.length));
    }  
      
    //"Match"
    assertEquals(expected.size(), result.size());
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), result.get(i));
      if(PRINT){
        System.out.println("Expected " +expected.get(i) + ", actual " +
            result.get(i));
      }
    }
  }
  
  
  public void testUpdate_MixedColumns(){
    if(PRINT) {
      System.out.println("\nUpdate_NewColumns");
    }
    byte [] col0 = Bytes.toBytes("col0");
    byte [] col1 = Bytes.toBytes("col1");
    byte [] col2 = Bytes.toBytes("col2");
    byte [] col3 = Bytes.toBytes("col3");
    byte [] col4 = Bytes.toBytes("col4");
    
    byte [] col5 = Bytes.toBytes("col5");
    byte [] col6 = Bytes.toBytes("col6");
    byte [] col7 = Bytes.toBytes("col7");
    byte [] col8 = Bytes.toBytes("col8");
    byte [] col9 = Bytes.toBytes("col9");
    
    //Create tracker
    List<MatchCode> expected = new ArrayList<MatchCode>();
    int size = 5;
    for(int i=0; i<size; i++){
      expected.add(MatchCode.INCLUDE);
    }
    for(int i=0; i<size; i++){
      expected.add(MatchCode.SKIP);
    }
    for(int i=0; i<size; i++){
      expected.add(MatchCode.INCLUDE);
    }
    for(int i=0; i<size; i++){
      expected.add(MatchCode.SKIP);
    }
    
    int maxVersions = 1;
    
    ColumnTracker wild = new WildcardColumnTracker(maxVersions);
        
    //Create "Scanner"
    List<byte[]> scanner = new ArrayList<byte[]>();
    scanner.add(col0);
    scanner.add(col2);
    scanner.add(col4);
    scanner.add(col6);
    scanner.add(col8);
    
    //Initialize result
    List<MatchCode> result = new ArrayList<MatchCode>(); 
    
    for(int i=0; i<2; i++){
      for(byte [] col : scanner){
        result.add(wild.checkColumn(col, 0, col.length));
      }
      wild.update();
    }

    //Create "Scanner1"
    List<byte[]> scanner1 = new ArrayList<byte[]>();
    scanner1.add(col1);
    scanner1.add(col3);
    scanner1.add(col5);
    scanner1.add(col7);
    scanner1.add(col9);
    for(byte [] col : scanner1){
      result.add(wild.checkColumn(col, 0, col.length));
    }
    wild.update();

    //Scanner again
    for(byte [] col : scanner){
      result.add(wild.checkColumn(col, 0, col.length));
    }  
      
    //"Match"
    assertEquals(expected.size(), result.size());
    
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), result.get(i));
      if(PRINT){
        System.out.println("Expected " +expected.get(i) + ", actual " +
            result.get(i));
      }
    }
  }
  
  
  
}
