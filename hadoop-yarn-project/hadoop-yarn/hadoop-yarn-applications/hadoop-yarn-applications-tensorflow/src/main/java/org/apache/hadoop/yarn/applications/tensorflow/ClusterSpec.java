package org.apache.hadoop.yarn.applications.tensorflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.util.*;

public class ClusterSpec {

    private static final Log LOG = LogFactory.getLog(ClusterSpec.class);
    private Map<String, TFWorkerAddress> workers = null;
    private Map<String, TFParamServerAddress> paramServers = null;
    private TFWorkerAddress tfMasterNode = null;
    private int serverPortNext = PORT_FLOOR;

    private static final int PORT_FLOOR = 20000;
    private static final int PORT_CEILING = 25000;

    public static final String WORKER = "worker";
    public static final String PS = "ps";

    public static ClusterSpec makeClusterSpec() {
        return new ClusterSpec();
    }

    private ClusterSpec() {
        workers = new HashMap<>();
        paramServers = new HashMap<>();
        serverPortNext = PORT_FLOOR + ((int)(Math.random() * (PORT_CEILING - PORT_FLOOR)) + 1);
    }

    private int nextRandomPort() {
        int port = serverPortNext;
        serverPortNext = serverPortNext + 2;
        return port;
    }

    private int maxTaskIndexOfWorkerInSameNode(String hostName) {
        int baseIndex = 0;
        for (TFWorkerAddress sv : workers.values()) {
            if (sv.getAddress() == hostName && sv.getTaskIndex() > baseIndex) {
                baseIndex = sv.getTaskIndex();
            }
        }

        return baseIndex;
    }

    public void addWorkerSpec(String containerId, String hostName) {

        TFWorkerAddress server = new TFWorkerAddress(this, hostName, nextRandomPort(), this.workers.size());
        if (tfMasterNode == null) {
            tfMasterNode = server;
        }
        this.workers.put(containerId, server);
    }

    private int maxTaskIndexOfPsInSameNode(String hostName) {
        int baseIndex = 0;
        for (TFParamServerAddress sv : paramServers.values()) {
            if (sv.getAddress() == hostName && sv.getTaskIndex() > baseIndex) {
                baseIndex = sv.getTaskIndex();
            }
        }

        return baseIndex;
    }

    public void addPsSpec(String containerId, String hostName) {
        TFParamServerAddress server = new TFParamServerAddress(this, hostName, nextRandomPort(), this.paramServers.size());
        this.paramServers.put(containerId, server);
    }

    public String getMasterNodeAddress() {
        if (tfMasterNode == null) {
            return  null;
        }
        return tfMasterNode.getAddress();
    }

    public int getMasterNodePort() {
        if (tfMasterNode == null) {
            return  0;
        }
        return tfMasterNode.getPort();
    }

    public boolean isWorker(String containerId) {
        return this.workers.containsKey(containerId);
    }

    public boolean isPs(String containerId) {
        return this.paramServers.containsKey(containerId);
    }

    public TFServerAddress getServerAddress(String containerId) {
        TFServerAddress server = this.workers.get(containerId);
        if (server == null) {
            LOG.info(containerId + " is not a worker" );
            server = this.paramServers.get(containerId);
        }

        return server;
    }

    @Override
    public String toString() {
        String worker_array = "";
        for (TFWorkerAddress wk : workers.values()) {
            worker_array += wk.getAddress() + ":" + wk.getPort() + ",";
        }
        String ps_array = "";
        for (TFParamServerAddress ps : paramServers.values()) {
            ps_array += ps.getAddress() + ":" + ps.getPort() + ",";
        }

        String cp = "";
        if (!worker_array.equals("")) {
            cp += "worker : [" + worker_array + "],";
        }

        if (!ps_array.equals("")) {
            cp += "ps : [" + ps_array + "]";
        }
        return  cp;
    }


    public String getJsonString() throws JsonProcessingException {
        Map<String, List<String>> cluster = new HashMap<>();

        if (!this.workers.isEmpty()) {
            List<String> servers = new ArrayList<String>();
            for (TFWorkerAddress s : this.workers.values()) {
                String addr = "" + s.getAddress() + ":" + s.getPort();
                servers.add(addr);
            }
            cluster.put(WORKER, servers);
        }

        if (!this.paramServers.isEmpty()) {
            List<String> servers = new ArrayList<String>();
            for (TFParamServerAddress s : this.paramServers.values()) {
                String addr = "" + s.getAddress() + ":" + s.getPort();
                servers.add(addr);
            }
            cluster.put(PS, servers);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        String json = null;
        json = objectMapper.writeValueAsString(cluster);
        return json;
    }

    public String getBase64EncodedJsonString() throws JsonProcessingException {
        byte[] data = getJsonString().getBytes();
        Base64 encoder = new Base64(0, null, true);
        return encoder.encodeToString(data);
    }

    public static String decodeJsonString(String base64String) {
        Base64 decoder = new Base64(0, null, true);
        byte[] data = decoder.decode(base64String);
        return new String(data);
    }


    public static Map<String, List<String>> toClusterMap(String clusterString) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, List<String>> cluster = null;
        cluster = objectMapper.readValue(clusterString, Map.class);

        return cluster;
    }

    public void testClusterString() {
        LOG.info("clusterspec: " + this.toString());
        try {
            LOG.info("clusterspec JsonString: " + this.getJsonString());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        try {
            LOG.info("clusterspec encodeJsonString: " + this.getBase64EncodedJsonString());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        String base64DecodedString = null;
        try {
            base64DecodedString = ClusterSpec.decodeJsonString(this.getBase64EncodedJsonString());
            LOG.info("clusterspec decodeJsonString: " + base64DecodedString);
            if (base64DecodedString.equals(this.getJsonString())) {
                LOG.info("raw and decode is equal!");
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


        try {
            Map<String, List<String>> cs = ClusterSpec.toClusterMap(base64DecodedString);
            if (cs.containsKey(WORKER)) {
                for (String s : cs.get(WORKER)) {
                    LOG.info(s);
                }
            }

            if (cs.containsKey(PS)) {
                for (String s : cs.get(PS)) {
                    LOG.info(s);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}