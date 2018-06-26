'use strict';

const elasticApi = require('@terascope/elasticsearch-api');

function newReader(context, opConfig, executionConfig, client) {
    return (msg, logger) => {
        const elasticsearch = elasticApi(client, logger, opConfig);
        const query = elasticsearch.buildQuery(opConfig, msg);
        return elasticsearch.search(query);
    };
}

module.exports = newReader;
