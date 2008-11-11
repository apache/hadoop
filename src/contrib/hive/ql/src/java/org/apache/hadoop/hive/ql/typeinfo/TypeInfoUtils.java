package org.apache.hadoop.hive.ql.typeinfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
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
    
  public static String getFieldSchemaTypeFromTypeInfo(TypeInfo typeInfo) {
    switch(typeInfo.getCategory()) {
      case PRIMITIVE: {
        return ObjectInspectorUtils.getClassShortName(typeInfo.getPrimitiveClass());
      }
      case LIST: {
        String elementType = getFieldSchemaTypeFromTypeInfo(typeInfo.getListElementTypeInfo());
        return org.apache.hadoop.hive.serde.Constants.LIST_TYPE_NAME + "<" + elementType + ">";
      }
      case MAP: {
        String keyType = getFieldSchemaTypeFromTypeInfo(typeInfo.getMapKeyTypeInfo());
        String valueType = getFieldSchemaTypeFromTypeInfo(typeInfo.getMapValueTypeInfo());
        return org.apache.hadoop.hive.serde.Constants.MAP_TYPE_NAME + "<" +
          keyType + "," + valueType + ">";
      }
      case STRUCT: {
        throw new RuntimeException("Complex struct type not supported!");
      }
      default: {
        throw new RuntimeException("Unknown type!");
      }
    }
  }
  
  /**
   * Convert TypeInfo to FieldSchema. 
   */
  public static FieldSchema getFieldSchemaFromTypeInfo(String fieldName, TypeInfo typeInfo) {
    return new FieldSchema(
        fieldName, getFieldSchemaTypeFromTypeInfo(typeInfo), "generated by TypeInfoUtils.getFieldSchemaFromTypeInfo"
    );
  }

  /**
   * The mapping from type name in DDL to the Java class. 
   */
  public static final Map<String, Class<?>> TypeNameToClass = new HashMap<String, Class<?>>();
  static {
    TypeNameToClass.put(Constants.BOOLEAN_TYPE_NAME, Boolean.class);
    TypeNameToClass.put(Constants.TINYINT_TYPE_NAME, Byte.class);
    TypeNameToClass.put(Constants.SMALLINT_TYPE_NAME, Short.class);
    TypeNameToClass.put(Constants.INT_TYPE_NAME, Integer.class);
    TypeNameToClass.put(Constants.BIGINT_TYPE_NAME, Long.class);
    TypeNameToClass.put(Constants.FLOAT_TYPE_NAME, Float.class);
    TypeNameToClass.put(Constants.DOUBLE_TYPE_NAME, Double.class);
    TypeNameToClass.put(Constants.STRING_TYPE_NAME, String.class);
    TypeNameToClass.put(Constants.DATE_TYPE_NAME, java.sql.Date.class);
    // These types are not supported yet. 
    // TypeNameToClass.put(Constants.DATETIME_TYPE_NAME);
    // TypeNameToClass.put(Constants.TIMESTAMP_TYPE_NAME);
  }
  
  /**
   * Return the primitive type corresponding to the field schema
   * @param field The field schema
   * @return The TypeInfo object, or null if the field is not a primitive type.
   */
  public static TypeInfo getPrimitiveTypeInfoFromFieldSchema(FieldSchema field) {
    String type = field.getType();
    
    Class<?> c = TypeNameToClass.get(type);
    return c == null ? null : TypeInfoFactory.getPrimitiveTypeInfo(c);
  }
}
