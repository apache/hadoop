package org.apache.hadoop.io.serial;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serial.lib.SerializationMetadata.TypedSerializationMetadata;
import org.yaml.snakeyaml.Yaml;

/**
 * An abstract base class for serializers that handle types under a given
 * parent type. Generally, their metadata consists of the class name of the
 * specific type that is being serialized.
 * <p>
 * Typically, TypedSerializations have two types. The first is the base type,
 * which is the static parent type that it can serialize. The other is the
 * specific type that this instance is current serializing.
 * @param <T> the base type that a given class of Serializers will serialize.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class TypedSerialization<T> extends Serialization<T> {
  protected Class<? extends T> specificType;
  
  protected TypedSerialization() {
  }
  
  protected TypedSerialization(Class<? extends T> specificType) {
    this.specificType = specificType;
  }

  /**
   * Get the base class that this method of serialization can handle.
   * @return the base class
   */
  public abstract Class<T> getBaseType();
  
  public void setSpecificType(Class<? extends T> cls) {
    specificType = cls;
  }

  public Class<? extends T> getSpecificType() {
    return specificType;
  }
  
  /**
   * Can this serialization serialize/deserialize a given class
   * @param candidateClass the class in question
   * @return true if the class can be serialized
   */
  public boolean accept(Class<?> candidateClass) {
    return getBaseType().isAssignableFrom(candidateClass);
  }

  /**
   * Read the specific class as the metadata.
   * @throws IOException when class not found or the deserialization fails
   */
  @Override
  public void deserializeSelf(InputStream in, 
                              Configuration conf) throws IOException {
    TypedSerializationMetadata data = TypedSerializationMetadata.parseFrom(in);
    if (data.hasTypename()) {
      setSpecificTypeByName(data.getTypename());
    }
  }

  /**
   * Write the specific class name as the metadata.
   */
  @Override
  public void serializeSelf(OutputStream out) throws IOException {
    TypedSerializationMetadata.newBuilder().
      setTypename(specificType == null ? "" : specificType.getName()).
      build().writeTo(out);
  }

  private static final String CLASS_ATTRIBUTE = "class";

  @SuppressWarnings("unchecked")
  @Override
  public void fromString(String meta) throws IOException {
    Yaml yaml = new Yaml();
    Map<String, String> map = (Map<String,String>) yaml.load(meta);
    String cls = map.get(CLASS_ATTRIBUTE);
    setSpecificTypeByName(cls);
  }

  @SuppressWarnings("unchecked")
  private void setSpecificTypeByName(String name) throws IOException {
    if (name == null || name.length() == 0) {
      specificType = null;
    } else {
      try {
        setSpecificType((Class<? extends T>) Class.forName(name));
      } catch (ClassNotFoundException e) {
        throw new IOException("serializer class not found " + name, e);
      }
    }
  }

  public String toString() {
    Yaml yaml = new Yaml();
    Map<String,String> map = new HashMap<String,String>();
    if (specificType != null) {
      map.put(CLASS_ATTRIBUTE, specificType.getName());
    }
    return yaml.dump(map);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object right) {
    if (this == right) {
      return true;
    } else if (right == null || right.getClass() != getClass()) {
      return false;
    } else {
      TypedSerialization<T> rightTyped = (TypedSerialization<T>) right;
      return specificType == rightTyped.specificType;
    }
  }
  
  @Override
  public int hashCode() {
    return specificType == null ? 42 : specificType.hashCode();
  }
  
  @Override
  public TypedSerialization<T> clone() {
    TypedSerialization<T> result = (TypedSerialization<T>) super.clone();
    result.specificType = specificType;
    return result;
  }
}
