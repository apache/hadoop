package org.apache.hadoop.hdfs.protocol;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Internal class similar to an {@link EncryptionZone} which also holds a
 * unique id. Used to implement batched listing of encryption zones.
 */
@InterfaceAudience.Private
public class EncryptionZoneWithId extends EncryptionZone {

  final long id;

  public EncryptionZoneWithId(String path, String keyName, long id) {
    super(path, keyName);
    this.id = id;
  }

  public long getId() {
    return id;
  }

  EncryptionZone toEncryptionZone() {
    return new EncryptionZone(getPath(), getKeyName());
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 29)
        .append(super.hashCode())
        .append(id)
        .toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    EncryptionZoneWithId that = (EncryptionZoneWithId) o;

    if (id != that.id) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return "EncryptionZoneWithId [" +
        "id=" + id +
        ", " + super.toString() +
        ']';
  }
}
