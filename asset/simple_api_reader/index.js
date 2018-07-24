'use strict';

const http = require('http');
const https = require('https');

const Promise = require('bluebird');
const _ = require('lodash');

// This is not ideal.
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

function getOpConfig(job, name) {
    return job.operations.find(op => op._op === name);
}

const MODULE_NAME = 'simple_api_reader';

function createClient(context, opConfig) {
    // NOTE: currently we are not supporting id based reader queries
    // NOTE: currently we do no have access to _type or _id of each doc
    const { logger } = context;

    function apiSearch(queryConfig) {
        function parseQueryConfig(mustArray) {
            const queryOptions = {
                query_string: _parseEsQ,
                range: _parseDate,
            };
            let geoQuery = _parseGeoQuery();
            if (geoQuery.length > 0) geoQuery = `&${geoQuery}`;
            let query = '';

            if (mustArray) {
                mustArray.forEach((queryAction) => {
                    _.forOwn(queryAction, (config, key) => {
                        const queryFn = queryOptions[key];
                        if (queryFn) {
                            const queryStr = queryFn(config);
                            if (query.length) {
                                query = `${query} AND ${queryStr}`;
                            } else {
                                query = queryStr;
                            }
                        }
                    });
                });
            } else {
                // TODO: review this area
                // get default date query
                query = _parseEsQ();
                // query = _parseDate(null);
            }
            // geo sort will be taken care of in the teraserver search api
            let sort = '';
            if (queryConfig.body && queryConfig.body.sort && queryConfig.body.sort.length > 0) {
                sort = `&sort=${opConfig.date_field_name}:${queryConfig.body.sort[0][opConfig.date_field_name].order}`; // {"date":{"order":"asc"}}
            }

            let { size } = queryConfig;
            if (size === undefined) {
                ({ size } = opConfig);
            }
            const initialQuery = `${opConfig.endpoint}/${opConfig.index}?token=${opConfig.token}&`;
            return `${initialQuery}q=${query}&size=${size}${sort}${geoQuery}`;
        }

        function _parseGeoQuery() {
            const {
                geo_box_top_left: geoBoxTopLeft,
                geo_box_bottom_right: geoBoxBottomRight,
                geo_point: geoPoint,
                geo_distance: geoDistance,
                geo_sort_point: geoSortPoint,
                geo_sort_order: geoSortOrder,
                geo_sort_unit: geoSortUnit
            } = opConfig;
            const geoQuery = [];

            if (geoBoxTopLeft) geoQuery.push(`geo_box_top_left=${geoBoxTopLeft}`);
            if (geoBoxBottomRight) geoQuery.push(`geo_box_bottom_right=${geoBoxBottomRight}`);
            if (geoPoint) geoQuery.push(`geo_point=${geoPoint}`);
            if (geoDistance) geoQuery.push(`geo_distance=${geoDistance}`);
            if (geoSortPoint) geoQuery.push(`geo_sort_point=${geoSortPoint}`);
            if (geoSortOrder) geoQuery.push(`geo_sort_order=${geoSortOrder}`);
            if (geoSortUnit) geoQuery.push(`geo_sort_unit=${geoSortUnit}`);
            return geoQuery.join('&');
        }

        function _parseEsQ(op) {
            const { q } = queryConfig;
            const results = q || _.get(op, 'query', '');
            return results;
        }

        // needs queryConfig.body.query.range
        function _parseDate(op) {
            let range;
            if (op) {
                range = op;
            } else {
                ({ range } = queryConfig.body.query);
            }

            const dateStart = new Date(range[opConfig.date_field_name].gte);
            const dateEnd = new Date(range[opConfig.date_field_name].lt);

            // Teraslice date ranges are >= start and < end.
            return `${opConfig.date_field_name}:[${dateStart.toISOString()} TO ${dateEnd.toISOString()}}`;
        }

        return new Promise(((resolve, reject) => {
            const mustQuery = _.get(queryConfig, 'body.query.bool.must', undefined);
            const query = parseQueryConfig(mustQuery);

            let protocol = http;
            if (query.indexOf('https') === 0) protocol = https;

            // Hack to stub out the actual HTTP request
            if (context.__test_mocks) {
                protocol = context.__test_mocks;
            }

            const activeRequest = protocol.get(query, (response) => {
                let body = '';
                // consume response body
                response.on('data', (chunk) => {
                    body += chunk;
                });

                response.on('end', () => {
                    let fullResonse;
                    if (body.startsWith('<')) {
                        // If we get an HTML tag in the response then something broke.
                        return reject(`Error response from the API: ${body}`);
                    }

                    try {
                        fullResonse = JSON.parse(body);
                        if (fullResonse.error) {
                            return reject(fullResonse.error);
                        }
                    } catch (err) {
                        return reject(err);
                    }

                    // Simulate ES query response
                    let esResults = [];
                    if (fullResonse.results) {
                        esResults = _.map(fullResonse.results, result => ({ _source: result }));
                    }

                    return resolve({
                        hits: {
                            hits: esResults,
                            total: fullResonse.total
                        },
                        timed_out: false,
                        _shards: {
                            total: 1,
                            successful: 1,
                            failed: 0
                        }
                    });
                });

                response.resume();
            }).on('error', (e) => {
                logger.error(`simple_api_reader error: ${e.message}`);
                reject(e.message);
            }).on('socket', (socket) => {
                socket.setTimeout(opConfig.timeout);
                socket.on('timeout', () => {
                    activeRequest.abort();
                    reject('HTTP request timed out connecting to API endpoint.');
                });
            });
        }));
    }

    return {
        search(queryConfig) {
            return apiSearch(queryConfig);
        },
        count(queryConfig) {
            queryConfig.size = 0;

            return apiSearch(queryConfig);
        },
        cluster: {
            stats() {
                return new Promise(((resolve) => {
                    resolve({
                        nodes: {
                            versions: ['0.5']
                        }
                    });
                }));
            },
            getSettings() {
                return new Promise(((resolve) => {
                    const result = {};

                    result[opConfig.index] = {
                        settings: {
                            index: {
                                max_result_window: 100000
                            }
                        }
                    };

                    resolve(result);
                }));
            }
        }
    };
}

function newSlicer(context, executionContext, retryData, logger) {
    const opConfig = getOpConfig(executionContext.config, MODULE_NAME);
    const client = createClient(context, opConfig);

    return require('../elasticsearch_reader/elasticsearch_date_range/slicer')(context, opConfig, executionContext, retryData, logger, client);
}

function newReader(context, opConfig, jobConfig) {
    const client = createClient(context, opConfig);
    const reader = require('../elasticsearch_reader/elasticsearch_date_range/reader')(context, opConfig, jobConfig, client);
    // This is a work around for the issue of the elasticsearch_api
    // returning a number when size is 0.

    // TODO: review me!!!!
    return function getData(data, logger) {
        return Promise.resolve(reader(data, logger))
            .then((results) => {
                if (_.isNumber(results)) {
                    return [];
                }

                return results;
            });
    };
}

function schema() {
    const esSchema = require('../elasticsearch_reader').schema();
    const apiSchema = {
        endpoint: {
            doc: 'The base API endpoint to read from: i.e. http://yourdomain.com/api/v1',
            default: '',
            format: 'required_String'
        },
        token: {
            doc: 'API access token for making requests',
            default: '',
            format: 'required_String'
        },
        timeout: {
            doc: 'Time in milliseconds to wait for a connection to timeout.',
            default: 300000
        },
    };

    return _.assign({}, esSchema, apiSchema);
}

module.exports = {
    newReader,
    newSlicer,
    schema,
    createClient
};
