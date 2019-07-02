'use strict';

const { ConvictSchema } = require('@terascope/job-components');

class Schema extends ConvictSchema {
    build() {
        return {
            index: {
                doc: 'name of elasticsearch index',
                default: '',
                format: 'required_String'
            },
            type: {
                doc: 'type of the elasticsearch data string',
                default: '',
                format: 'required_String'
            },
            concurrency: {
                doc: 'number of cuncurrent requests to elasticsearch',
                default: 100
            },
            source_fields: {
                doc: 'fields to retreive from elasticsearch, array of fields, defaults to all fields',
                default: []
            },
            chunk_size: {
                doc: 'how many docs to send in the elasticsearch mget request at a time, defaults to 2500',
                default: 2500
            },
            persist: {
                doc: 'If set to true will save state in storage for mset, doest not apply to set, defaults to false',
                default: false
            },
            persist_field: {
                doc: ' If persist is true this option is the name of the metadata key field that will be used to make the id of the doc in the es bulk update, defaults to _id',
                default: '',
                format: 'optional_String'
            },
            connection: {
                doc: 'elasticsearch connection',
                default: 'default'
            },
            cache_size: {
                doc: 'max number of items to store in the cache (not memory size), default to 1000000',
                default: 1000000
            },
            max_age: {
                doc: 'length of time before a record expires in milliseconds positive integer, default: 24 hours',
                default: 24 * 3600 * 1000
            }
        };
    }
}

module.exports = Schema;
