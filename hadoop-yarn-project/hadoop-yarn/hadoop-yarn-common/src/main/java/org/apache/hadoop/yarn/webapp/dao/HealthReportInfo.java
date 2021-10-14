package org.apache.hadoop.yarn.webapp.dao;

import org.apache.hadoop.yarn.health.HealthReport;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@XmlRootElement(name = "report")
public class HealthReportInfo {
    @XmlElement(name = "componentHealth")
    private List<HealthReportItemInfo> items = new ArrayList<>();

    public HealthReportInfo() {
    }

    public HealthReportInfo(List<HealthReport> reports) {
        if (reports != null) {
            items = reports.stream().map(HealthReportItemInfo::new).collect(Collectors.toList());
        }
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static class HealthReportItemInfo {
        public String name;
        public boolean healthy;
        public String workState;
        public long updateTime;
        public String diagnostics;
        public Map<String, String> metrics;

        HealthReportItemInfo() {
        }

        HealthReportItemInfo(HealthReport report) {
            name = report.getName();
            healthy = report.isHealthy();
            workState = report.getWorkState().name();
            updateTime = report.getUpdateTime();
            diagnostics = report.getDiagnostics();
            metrics = report.getMetrics().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey, x -> x.getValue().toString()
            ));
        }
    }
}
