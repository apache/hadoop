/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.api.records;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlElement;
import java.io.Serializable;
import java.util.Objects;

/**
 * The kerberos principal of the service.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "The kerberos principal of the service.")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KerberosPrincipal implements Serializable {
  private static final long serialVersionUID = -6431667195287650037L;

  @JsonProperty("principal_name")
  @XmlElement(name = "principal_name")
  private String principalName = null;

  @JsonProperty("keytab")
  @XmlElement(name = "keytab")
  private String keytab = null;

  public KerberosPrincipal principalName(String principalName) {
    this.principalName = principalName;
    return this;
  }

  /**
   * The principal name of the service.
   *
   * @return principalName
   **/
  @ApiModelProperty(value = "The principal name of the service.")
  public String getPrincipalName() {
    return principalName;
  }

  public void setPrincipalName(String principalName) {
    this.principalName = principalName;
  }

  public KerberosPrincipal keytab(String keytab) {
    this.keytab = keytab;
    return this;
  }

  /**
   * The URI of the kerberos keytab. It supports two schemes \&quot;
   * hdfs\&quot; and \&quot;file\&quot;. If the URI starts with \&quot;
   * hdfs://\&quot; scheme, it indicates the path on hdfs where the keytab is
   * stored. The keytab will be localized by YARN and made available to AM in
   * its local directory. If the URI starts with \&quot;file://\&quot;
   * scheme, it indicates a path on the local host presumbaly installed by
   * admins upfront.
   *
   * @return keytab
   **/
  @ApiModelProperty(value = "The URI of the kerberos keytab. It supports two " +
      "schemes \"hdfs\" and \"file\". If the URI starts with \"hdfs://\" " +
      "scheme, it indicates the path on hdfs where the keytab is stored. The " +
      "keytab will be localized by YARN and made available to AM in its local" +
      " directory. If the URI starts with \"file://\" scheme, it indicates a " +
      "path on the local host where the keytab is presumbaly installed by " +
      "admins upfront. ")
  public String getKeytab() {
    return keytab;
  }

  public void setKeytab(String keytab) {
    this.keytab = keytab;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KerberosPrincipal kerberosPrincipal = (KerberosPrincipal) o;
    return Objects.equals(this.principalName, kerberosPrincipal
        .principalName) &&
        Objects.equals(this.keytab, kerberosPrincipal.keytab);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principalName, keytab);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class KerberosPrincipal {\n");

    sb.append("    principalName: ").append(toIndentedString(principalName))
        .append("\n");
    sb.append("    keytab: ").append(toIndentedString(keytab)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

