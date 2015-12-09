import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.JSONAPISerializer.extend({
    internalNormalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      
      var fixedPayload = {
        id: payload.containerId,
        type: primaryModelClass.modelName, // yarn-app
        attributes: {
          allocatedMB: payload.allocatedMB,
          allocatedVCores: payload.allocatedVCores,
          assignedNodeId: payload.assignedNodeId,
          priority: payload.priority,
          startedTime: Converter.timeStampToDate(payload.startedTime),
          finishedTime: Converter.timeStampToDate(payload.finishedTime),
          elapsedTime: payload.elapsedTime,
          logUrl: payload.logUrl,
          containerExitStatus: payload.containerExitStatus,
          containerState: payload.containerState,
          nodeHttpAddress: payload.nodeHttpAddress
        }
      };

      return fixedPayload;
    },

    normalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      var p = this.internalNormalizeSingleResponse(store, 
        primaryModelClass, payload, id, requestType);
      return { data: p };
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id,
      requestType) {
      // return expected is { data: [ {}, {} ] }
      var normalizedArrayResponse = {};

      if (payload && payload.container) {
        // payload has apps : { app: [ {},{},{} ]  }
        // need some error handling for ex apps or app may not be defined.
        normalizedArrayResponse.data = payload.container.map(singleContainer => {
          return this.internalNormalizeSingleResponse(store, primaryModelClass,
            singleContainer, singleContainer.id, requestType);
        }, this);
        return normalizedArrayResponse;  
      }

      normalizedArrayResponse.data = [];
      return normalizedArrayResponse;
    }
});