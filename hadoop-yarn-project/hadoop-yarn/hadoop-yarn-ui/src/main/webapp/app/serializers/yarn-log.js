import DS from 'ember-data';

export default DS.JSONAPISerializer.extend({
  internalNormalizeSingleResponse(store, primaryModelClass, payload, containerId, nodeId) {
    var fixedPayload = {
      id: "yarn_log_" + payload.fileName + "_" + Date.now(),
      type: primaryModelClass.modelName,
      attributes: {
        fileName: payload.fileName,
        fileSize: payload.fileSize,
        lastModifiedTime: payload.lastModifiedTime,
        containerId: containerId,
        nodeId: nodeId
      }
    };
    return fixedPayload;
  },

  normalizeArrayResponse(store, primaryModelClass, payload/*, id, requestType*/) {
    var normalizedArrayResponse = {
      data: []
    };
    if (payload && payload.containerLogsInfo && payload.containerLogsInfo.containerLogInfo) {
      normalizedArrayResponse.data = payload.containerLogsInfo.containerLogInfo.map((paylog) => {
        return this.internalNormalizeSingleResponse(store, primaryModelClass, paylog,
          payload.containerLogsInfo.containerId, payload.containerLogsInfo.nodeId);
      });
    }
    return normalizedArrayResponse;
  }
});
