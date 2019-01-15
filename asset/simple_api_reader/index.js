'use strict';

const { getOpConfig } = require('@terascope/job-components');
const Promise = require('bluebird');
const _ = require('lodash');
const got = require('got');

// eslint-disable-next-line
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

function request({ uri }) {
    return got(uri, { json: true }).then(response => response.body);
}

const MODULE_NAME = 'simple_api_reader';

function createClient(context, opConfig) {
    // NOTE: currently we are not supporting id based reader queries
    // NOTE: currently we do no have access to _type or _id of each doc
    const { logger } = context;
    const fetchData = context.__test_mocks || request;

    function makeRequest(uri) {
        return new Promise((resolve, reject) => {
            const ref = setTimeout(() => reject(new Error('HTTP request timed out connecting to API endpoint.')), opConfig.timeout);

            fetchData({ uri, json: true })
                .then((results) => {
                    clearTimeout(ref);
                    resolve(results);
                })
                .catch((err) => {
                    clearTimeout(ref);
                    reject(err);
                });
        });
    }

    function apiSearch(queryConfig) {
        const fields = _.get(queryConfig, '_source', null);
        const fieldsQuery = fields ? `&fields=${fields.join(',')}` : '';
        const mustQuery = _.get(queryConfig, 'body.query.bool.must', null);
        const searchQuery = parseQueryConfig(mustQuery);

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
                            let queryStr = queryFn(config);
                            if (key !== 'range') queryStr = `(${queryStr})`;

                            if (query.length) {
                                query = `${query} AND ${queryStr}`;
                            } else {
                                query = queryStr;
                            }
                        }
                    });
                });
            } else {
                query = _parseEsQ();
            }
            // geo sort will be taken care of in the teraserver search api
            let sort = '';
            if (queryConfig.body && queryConfig.body.sort && queryConfig.body.sort.length > 0) {
                queryConfig.body.sort.forEach((sortType) => {
                    // We are checking for date sorts, geo sorts are handled by _parseGeoQuery
                    if (sortType[opConfig.date_field_name]) {
                        // {"date":{"order":"asc"}}
                        sort = `&sort=${opConfig.date_field_name}:${queryConfig.body.sort[0][opConfig.date_field_name].order}`;
                    }
                });
            }
            let { size } = queryConfig;
            if (size == null) {
                ({ size } = opConfig);
            }
            const initialQuery = `${opConfig.endpoint}/${opConfig.index}?token=${opConfig.token}&`;
            return `${initialQuery}q=${query}&size=${size}${sort}${geoQuery}${fieldsQuery}`;
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

        function callTeraserver(uri) {
            return Promise.resolve()
                .then(() => makeRequest(uri))
                .then((response) => {
                    let esResults = [];
                    if (response.results) {
                        esResults = _.map(response.results, result => ({ _source: result }));
                    }

                    return ({
                        hits: {
                            hits: esResults,
                            total: response.total
                        },
                        timed_out: false,
                        _shards: {
                            total: 1,
                            successful: 1,
                            failed: 0
                        }
                    });
                })
                .catch((err) => {
                    logger.error(`error while calling endpoint ${uri}, error: ${err.message}`);
                    return Promise.reject(err);
                });
        }

        return callTeraserver(searchQuery);
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
    return require('../elasticsearch_reader/elasticsearch_date_range/reader')(context, opConfig, jobConfig, client);
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
