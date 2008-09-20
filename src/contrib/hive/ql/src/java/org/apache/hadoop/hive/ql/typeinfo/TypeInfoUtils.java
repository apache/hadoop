package org.apache.hadoop.hive.ql.typeinfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class TypeInfoUtils {
  
  static HashMap<TypeInfo, ObjectInspector> cachedStandardObjectInspector = new HashMap<TypeInfo, ObjectInspector>();
  /**
   * Returns the standard object inspector that can be used to translate an object of that typeInfo
   * to a standard object type.  
   */
  public static ObjectInspector getStandardObjectInspectorFromTypeInfo(TypeInfo typeInfo) {
    ObjectInspector result = cachedStandardObjectInspector.get(typeInfo);
    if (result == null) {
      switch(typeInfo.getCategory()) {
        case PRIMITIVE: {
          result = ObjectInspectorFactory.getStandardPrimitiveObjectInspector(typeInfo.getPrimitiveClass());
          break;
        }
        case LIST: {
          ObjectInspector elementObjectInspector = getStandardObjectInspectorFromTypeInfo(typeInfo.getListElementTypeInfo());
          result = ObjectInspectorFactory.getStandardListObjectInspector(elementObjectInspector);
          break;
        }
        case MAP: {
          ObjectInspector keyObjectInspector = getStandardObjectInspectorFromTypeInfo(typeInfo.getMapKeyTypeInfo());
          ObjectInspector valueObjectInspector = getStandardObjectInspectorFromTypeInfo(typeInfo.getMapValueTypeInfo());
          result = ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
          break;
        }
        case STRUCT: {
          List<String> fieldNames = typeInfo.getAllStructFieldNames();
          List<TypeInfo> fieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();
          List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(fieldTypeInfos.size());
          for(int i=0; i<fieldTypeInfos.size(); i++) {
            fieldObjectInspectors.add(getStandardObjectInspectorFromTypeInfo(fieldTypeInfos.get(i)));
          }
          result = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldObjectInspectors);
          break;
        }
        default: {
          result = null;
        }
      }
      cachedStandardObjectInspector.put(typeInfo, result);
    }
    return result;
  }

  
  /**
   * Get the TypeInfo object from the ObjectInspector object by recursively going into the
   * ObjectInspector structure.
   */
  public static TypeInfo getTypeInfoFromObjectInspector(ObjectInspector oi) {
//    OPTIMIZATION for later.
//    if (oi instanceof TypeInfoBasedObjectInspector) {
//      TypeInfoBasedObjectInspector typeInfoBasedObjectInspector = (ObjectInspector)oi;
//      return typeInfoBasedObjectInspector.getTypeInfo();
//    }
    
    // Recursively going into ObjectInspector structure
    TypeInfo result = null;
    switch (oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi =(PrimitiveObjectInspector)oi;
        result = TypeInfoFactory.getPrimitiveTypeInfo(poi.getPrimitiveClass());
        break;
      }
      case LIST: {
        ListObjectInspector loi = (ListObjectInspector)oi;
        result = TypeInfoFactory.getListTypeInfo(
            getTypeInfoFromObjectInspector(loi.getListElementObjectInspector()));
        break;
      }
      case MAP: {
        MapObjectInspector moi = (MapObjectInspector)oi;
        result = TypeInfoFactory.getMapTypeInfo(
            getTypeInfoFromObjectInspector(moi.getMapKeyObjectInspector()),
            getTypeInfoFromObjectInspector(moi.getMapValueObjectInspector()));
        break;
      }
      case STRUCT: {
        StructObjectInspector soi = (StructObjectInspector)oi;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<String> fieldNames = new ArrayList<String>(fields.size());
        List<TypeInfo> fieldTypeInfos = new ArrayList<TypeInfo>(fields.size());
        for(StructField f : fields) {
          fieldNames.add(f.getFieldName());
          fieldTypeInfos.add(getTypeInfoFromObjectInspector(f.getFieldObjectInspector()));
        }
        result = TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
        break;
      }
      default: {
        throw new RuntimeException("Unknown ObjectInspector category!");
      }
    }
    return result;
  }
    
}
