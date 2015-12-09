import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.JSONAPISerializer.extend({
    internalNormalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      
      if (payload.appAttempt) {
        payload = payload.appAttempt;  
      }
      
      var fixedPayload = {
        id: payload.appAttemptId,
        type: primaryModelClass.modelName, // yarn-app
        attributes: {
          startTime: Converter.timeStampToDate(payload.startTime),
          finishedTime: Converter.timeStampToDate(payload.finishedTime),
          containerId: payload.containerId,
          nodeHttpAddress: payload.nodeHttpAddress,
          nodeId: payload.nodeId,
          state: payload.nodeId,
          logsLink: payload.logsLink
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

      // payload has apps : { app: [ {},{},{} ]  }
      // need some error handling for ex apps or app may not be defined.
      normalizedArrayResponse.data = payload.appAttempts.appAttempt.map(singleApp => {
        return this.internalNormalizeSingleResponse(store, primaryModelClass,
          singleApp, singleApp.id, requestType);
      }, this);
      return normalizedArrayResponse;
    }
});