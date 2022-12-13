package org.apache.hadoop.yarn.server.router.webapp.dao;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FederationSchedulerTypeInfo extends SchedulerTypeInfo {
  @XmlElement(name = "subCluster")
  private List<SchedulerTypeInfo> list = new ArrayList<>();

  public FederationSchedulerTypeInfo() {
  } // JAXB needs this

  public FederationSchedulerTypeInfo(ArrayList<SchedulerTypeInfo> list) {
    this.list = list;
  }

  public List<SchedulerTypeInfo> getList() {
    return list;
  }

  public void setList(List<SchedulerTypeInfo> list) {
    this.list = list;
  }
}
