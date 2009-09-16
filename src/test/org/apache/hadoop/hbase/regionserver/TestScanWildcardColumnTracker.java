package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.regionserver.QueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;

public class TestScanWildcardColumnTracker extends HBaseTestCase {

  final int VERSIONS = 2;
  
  public void testCheckColumn_Ok() {
    //Create a WildcardColumnTracker
    ScanWildcardColumnTracker tracker = 
      new ScanWildcardColumnTracker(VERSIONS);
    
    //Create list of qualifiers
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    qualifiers.add(Bytes.toBytes("qualifer1"));
    qualifiers.add(Bytes.toBytes("qualifer2"));
    qualifiers.add(Bytes.toBytes("qualifer3"));
    qualifiers.add(Bytes.toBytes("qualifer4"));
    
    //Setting up expected result
    List<MatchCode> expected = new ArrayList<MatchCode>();
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    
    List<MatchCode> actual = new ArrayList<MatchCode>();
    
    for(byte [] qualifier : qualifiers) {
      MatchCode mc = tracker.checkColumn(qualifier, 0, qualifier.length);
      actual.add(mc);
    }

    //Compare actual with expected
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  public void testCheckColumn_EnforceVersions() {
    //Create a WildcardColumnTracker
    ScanWildcardColumnTracker tracker = 
      new ScanWildcardColumnTracker(VERSIONS);
    
    //Create list of qualifiers
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    qualifiers.add(Bytes.toBytes("qualifer1"));
    qualifiers.add(Bytes.toBytes("qualifer1"));
    qualifiers.add(Bytes.toBytes("qualifer1"));
    qualifiers.add(Bytes.toBytes("qualifer2"));
    
    //Setting up expected result
    List<MatchCode> expected = new ArrayList<MatchCode>();
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.INCLUDE);
    expected.add(MatchCode.SKIP);
    expected.add(MatchCode.INCLUDE);
    
    List<MatchCode> actual = new ArrayList<MatchCode>();
    
    for(byte [] qualifier : qualifiers) {
      MatchCode mc = tracker.checkColumn(qualifier, 0, qualifier.length);
      actual.add(mc);
    }

    //Compare actual with expected
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  public void DisabledTestCheckColumn_WrongOrder() {
    //Create a WildcardColumnTracker
    ScanWildcardColumnTracker tracker = 
      new ScanWildcardColumnTracker(VERSIONS);
    
    //Create list of qualifiers
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    qualifiers.add(Bytes.toBytes("qualifer2"));
    qualifiers.add(Bytes.toBytes("qualifer1"));
    
    boolean ok = false;
    
    try {
      for(byte [] qualifier : qualifiers) {
        MatchCode mc = tracker.checkColumn(qualifier, 0, qualifier.length);
      }
    } catch (Exception e) {
      ok = true;
    }

    assertEquals(true, ok);
  }
  
}
