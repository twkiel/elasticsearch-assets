'use strict';

const elasticApi = require('@terascope/elasticsearch-api');
const { DataEntity } = require('@terascope/job-components');

function newReader(context, opConfig, executionConfig, client) {
    const queryConfig = Object.assign({}, opConfig, { full_response: true });

    return (msg, logger) => {
        const elasticsearch = elasticApi(client, logger, queryConfig);
        const query = elasticsearch.buildQuery(queryConfig, msg);

        return elasticsearch.search(query)
            .then(fullResponseObj => fullResponseObj.hits.hits.map((doc) => {
                const metadata = { _key: doc._id };
                return DataEntity.make(doc._source, metadata);
            }));
    };
}

module.exports = newReader;
