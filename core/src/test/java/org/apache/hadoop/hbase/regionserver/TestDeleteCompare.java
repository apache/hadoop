package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.regionserver.DeleteCompare.DeleteCode;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class TestDeleteCompare extends TestCase {

  //Cases to compare:
  //1. DeleteFamily and whatever of the same row
  //2. DeleteColumn and whatever of the same row + qualifier
  //3. Delete and the matching put
  //4. Big test that include starting on the wrong row and qualifier
  public void testDeleteCompare_DeleteFamily() {
    //Creating memstore
    Set<KeyValue> memstore = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 3, "d-c"));
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 2, "d-c"));
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 1, "d-c"));
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col2", 1, "d-c"));

    memstore.add(KeyValueTestUtil.create("row11", "fam", "col3", 3, "d-c"));
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col3", 2, "d-c"));
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col3", 1, "d-c"));

    memstore.add(KeyValueTestUtil.create("row21", "fam", "col1", 1, "d-c"));

    //Creating expected result
    List<DeleteCode> expected = new ArrayList<DeleteCode>();
    expected.add(DeleteCode.SKIP);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.SKIP);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DONE);

    KeyValue delete = KeyValueTestUtil.create("row11",
        "fam", "", 2, KeyValue.Type.DeleteFamily, "dont-care");
    byte [] deleteBuffer = delete.getBuffer();
    int deleteRowOffset = delete.getRowOffset();
    short deleteRowLen = delete.getRowLength();
    int deleteQualifierOffset = delete.getQualifierOffset();
    int deleteQualifierLen = delete.getQualifierLength();
    int deleteTimestampOffset = deleteQualifierOffset + deleteQualifierLen;
    byte deleteType = deleteBuffer[deleteTimestampOffset +Bytes.SIZEOF_LONG];
    
    List<DeleteCode> actual = new ArrayList<DeleteCode>();
    for(KeyValue mem : memstore){
    actual.add(DeleteCompare.deleteCompare(mem, deleteBuffer, deleteRowOffset,
        deleteRowLen, deleteQualifierOffset, deleteQualifierLen,
        deleteTimestampOffset, deleteType, KeyValue.KEY_COMPARATOR));
      
    }
    
    assertEquals(expected.size(), actual.size());
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  public void testDeleteCompare_DeleteColumn() {
    //Creating memstore
    Set<KeyValue> memstore = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 3, "d-c"));
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 2, "d-c"));
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 1, "d-c"));
    memstore.add(KeyValueTestUtil.create("row21", "fam", "col1", 1, "d-c"));


    //Creating expected result
    List<DeleteCode> expected = new ArrayList<DeleteCode>();
    expected.add(DeleteCode.SKIP);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DONE);
    
    KeyValue delete = KeyValueTestUtil.create("row11", "fam", "col1", 2,
        KeyValue.Type.DeleteColumn, "dont-care");
    byte [] deleteBuffer = delete.getBuffer();
    int deleteRowOffset = delete.getRowOffset();
    short deleteRowLen = delete.getRowLength();
    int deleteQualifierOffset = delete.getQualifierOffset();
    int deleteQualifierLen = delete.getQualifierLength();
    int deleteTimestampOffset = deleteQualifierOffset + deleteQualifierLen;
    byte deleteType = deleteBuffer[deleteTimestampOffset +Bytes.SIZEOF_LONG];
    
    List<DeleteCode> actual = new ArrayList<DeleteCode>();
    for(KeyValue mem : memstore){
    actual.add(DeleteCompare.deleteCompare(mem, deleteBuffer, deleteRowOffset,
        deleteRowLen, deleteQualifierOffset, deleteQualifierLen,
        deleteTimestampOffset, deleteType, KeyValue.KEY_COMPARATOR));
      
    }
    
    assertEquals(expected.size(), actual.size());
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  
  public void testDeleteCompare_Delete() {
    //Creating memstore
    Set<KeyValue> memstore = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 3, "d-c"));
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 2, "d-c"));
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 1, "d-c"));

    //Creating expected result
    List<DeleteCode> expected = new ArrayList<DeleteCode>();
    expected.add(DeleteCode.SKIP);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DONE);
    
    KeyValue delete = KeyValueTestUtil.create("row11", "fam", "col1", 2,
        KeyValue.Type.Delete, "dont-care");
    byte [] deleteBuffer = delete.getBuffer();
    int deleteRowOffset = delete.getRowOffset();
    short deleteRowLen = delete.getRowLength();
    int deleteQualifierOffset = delete.getQualifierOffset();
    int deleteQualifierLen = delete.getQualifierLength();
    int deleteTimestampOffset = deleteQualifierOffset + deleteQualifierLen;
    byte deleteType = deleteBuffer[deleteTimestampOffset +Bytes.SIZEOF_LONG];
    
    List<DeleteCode> actual = new ArrayList<DeleteCode>();
    for(KeyValue mem : memstore){
    actual.add(DeleteCompare.deleteCompare(mem, deleteBuffer, deleteRowOffset,
        deleteRowLen, deleteQualifierOffset, deleteQualifierLen,
        deleteTimestampOffset, deleteType, KeyValue.KEY_COMPARATOR));
    }
    
    assertEquals(expected.size(), actual.size());
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
    }
  }
  
  public void testDeleteCompare_Multiple() {
    //Creating memstore
    Set<KeyValue> memstore = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    memstore.add(KeyValueTestUtil.create("row11", "fam", "col1", 1, "d-c"));
    memstore.add(KeyValueTestUtil.create("row21", "fam", "col1", 4, "d-c"));
    memstore.add(KeyValueTestUtil.create("row21", "fam", "col1", 3, "d-c"));
    memstore.add(KeyValueTestUtil.create("row21", "fam", "col1", 2, "d-c"));
    memstore.add(KeyValueTestUtil.create("row21", "fam", "col1", 1,
        KeyValue.Type.Delete, "dont-care"));
    memstore.add(KeyValueTestUtil.create("row31", "fam", "col1", 1, "dont-care"));

    //Creating expected result
    List<DeleteCode> expected = new ArrayList<DeleteCode>();
    expected.add(DeleteCode.SKIP);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DELETE);
    expected.add(DeleteCode.DONE);

    KeyValue delete = KeyValueTestUtil.create("row21", "fam", "col1", 5,
        KeyValue.Type.DeleteColumn, "dont-care");
    byte [] deleteBuffer = delete.getBuffer();
    int deleteRowOffset = delete.getRowOffset();
    short deleteRowLen = delete.getRowLength();
    int deleteQualifierOffset = delete.getQualifierOffset();
    int deleteQualifierLen = delete.getQualifierLength();
    int deleteTimestampOffset = deleteQualifierOffset + deleteQualifierLen;
    byte deleteType = deleteBuffer[deleteTimestampOffset +Bytes.SIZEOF_LONG];
    
    List<DeleteCode> actual = new ArrayList<DeleteCode>();
    for(KeyValue mem : memstore){
    actual.add(DeleteCompare.deleteCompare(mem, deleteBuffer, deleteRowOffset,
        deleteRowLen, deleteQualifierOffset, deleteQualifierLen,
        deleteTimestampOffset, deleteType, KeyValue.KEY_COMPARATOR));
      
    }
    
    assertEquals(expected.size(), actual.size());
    for(int i=0; i<expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
    }
  }
}
