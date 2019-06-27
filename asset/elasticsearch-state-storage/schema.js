'use strict';

const { ConvictSchema } = require('@terascope/job-components');

class Schema extends ConvictSchema {
    build() {
        return {
            index: {
                default: 'index'
            },
            type: {
                default: 'type'
            },
            concurrency: {
                default: 100
            },
            source_fields: {
                default: []
            },
            chunk_size: {
                default: 2500
            },
            id_field: {
                default: 'id'
            },
            persist: {
                default: false
            },
            persist_field: {
                default: 'id'
            },
            connection: {
                default: 'default'
            }
        };
    }
}

module.exports = Schema;
