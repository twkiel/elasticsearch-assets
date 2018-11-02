'use strict';

const harness = require('@terascope/teraslice-op-test-harness');
const { DataEntity } = require('../asset/node_modules/@terascope/job-components');
const indexer = require('../asset/elasticsearch_index_selector');

describe('elasticsearch index selector', () => {
    const opTest = harness(indexer);

    it('has a schema and newProcessor method', () => {
        const processor = indexer;

        expect(processor).toBeDefined();
        expect(processor.newProcessor).toBeDefined();
        expect(processor.schema).toBeDefined();
        expect(typeof processor.newProcessor).toEqual('function');
        expect(typeof processor.schema).toEqual('function');
    });

    it('schema function returns on object, formatted to be used by convict', () => {
        const schema = indexer.schema();
        const type = Object.prototype.toString.call(schema);
        const keys = Object.keys(schema);

        expect(type).toEqual('[object Object]');
        expect(keys.length).toBeGreaterThan(0);
        expect(schema[keys[0]].default).toBeDefined();
    });

    it('new processor will throw if other config options are not present with timeseries', () => {
        const op1 = { timeseries: 'daily' };
        const op2 = { timeseries: 'daily', index_prefix: 'hello' };
        const op3 = { timeseries: 'daily', index_prefix: 'hello', date_field: 'created' };

        expect(() => {
            indexer.selfValidation(op1);
        }).toThrowError('elasticsearch_index_selector is mis-configured, if any of the following configurations are set: timeseries, index_prefix or date_field, they must all be used together, please set the missing parameters');
        expect(() => {
            indexer.selfValidation(op2);
        }).toThrowError('elasticsearch_index_selector is mis-configured, if any of the following configurations are set: timeseries, index_prefix or date_field, they must all be used together, please set the missing parameters');
        expect(() => {
            indexer.selfValidation(op3);
        }).not.toThrowError();
    });

    it('new processor will throw properly', () => {
        const job1 = {
            operations: [
                { _op: 'elasticsearch_reader' },
                { _op: 'elasticsearch_index_selector', type: 'someType' }
            ]
        };

        const job2 = {
            operations: [
                { _op: 'elasticsearch_reader' },
                { _op: 'elasticsearch_index_selector' }
            ]
        };

        expect(() => {
            indexer.crossValidation(job1);
        }).not.toThrowError('e');
        expect(() => {
            indexer.crossValidation(job2);
        }).toThrowError('type must be specified in elasticsearch index selector config if data is not a full response from elasticsearch');
    });

    it('newProcessor takes either an array or elasticsearch formatted data and returns an array', async () => {
        const opConfig = { _op: 'elasticsearch_index_selector', index: 'some_index', type: 'someType' };
        const data1 = opTest.data.arrayLike;
        const data2 = opTest.data.esLike;
        const test = await opTest.init({ opConfig });
        const [results1, results2] = await Promise.all([test.run(data1), test.run(data2)]);

        expect(Array.isArray(results1)).toBe(true);
        expect(results1.length > 0).toBe(true);
        expect(Array.isArray(results2)).toBe(true);
        expect(results2.length > 0).toBe(true);
    });

    it('it returns properly formatted data for bulk requests', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            delete: false
        };
        const results = await opTest.processData(opConfig, opTest.data.arrayLike);

        expect(results[0]).toEqual({ index: { _index: 'some_index', _type: 'events' } });
        expect(results[1]).toEqual(opTest.data.arrayLike[0]);
    });

    it('full_response still works', () => {
        const context = {};
        const opConfig = {
            index: 'some-index',
            type: 'events',
            full_response: true,
            delete: false
        };
        const jobConfig = { logger: 'im a fake logger' };
        const data = {
            hits: {
                hits: [
                    { _id: 'specialID', _source: { some: 'data' } }
                ]
            }
        };

        const fn = indexer.newProcessor(context, opConfig, jobConfig);
        const results = fn(data);
        expect(results[0]).toEqual({ index: { _index: 'some-index', _type: 'events', _id: 'specialID' } });
    });


    xit('preserve_id will keep the previous id from elasticsearch data', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            preserve_id: true,
            delete: false
        };
        const data = {
            hits: {
                hits: [
                    {
                        type: 'someType',
                        _index: 'some_index',
                        _id: 'specialID',
                        _source: { some: 'data' }
                    }
                ]
            }
        };
        const results = await opTest.processData(opConfig, data);

        expect(results[0]).toEqual({ index: { _index: 'some_index', _type: 'events', _id: 'specialID' } });
        expect(results[1]).toEqual({ some: 'data' });
    });

    it('preserve_id will work the DataEntity', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            preserve_id: true,
            delete: false
        };
        const data = [
            DataEntity.make({ some: 'data' }, { _key: 'specialID' })
        ];
        const results = await opTest.processData(opConfig, data);

        expect(results[0]).toEqual({ index: { _index: 'some_index', _type: 'events', _id: 'specialID' } });
        expect(results[1]).toEqual({ some: 'data' });
    });

    it('can set id to any field in data', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            id_field: 'name'
        };
        const data = [{ some: 'data', name: 'someName' }];
        const results = await opTest.processData(opConfig, data);

        expect(results[0]).toEqual({ index: { _index: 'some_index', _type: 'events', _id: 'someName' } });
        expect(results[1]).toEqual(data[0]);
    });

    it('can send an update request instead of index', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            id_field: 'name',
            update_fields: ['name'],
            delete: false,
            update: true
        };
        const data = [{ some: 'data', name: 'someName' }];
        const results = await opTest.processData(opConfig, data);

        expect(results[0]).toEqual({ update: { _index: 'some_index', _type: 'events', _id: 'someName' } });
        expect(results[1]).toEqual({ doc: { name: 'someName' } });
    });

    it('can send a delete request instead of index', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            id_field: 'name',
            delete: true
        };
        const data = [{ some: 'data', name: 'someName' }];
        const results = await opTest.processData(opConfig, data);

        expect(results[0]).toEqual({ delete: { _index: 'some_index', _type: 'events', _id: 'someName' } });
    });

    it('can upsert specified fields by passing in an array of keys matching the document', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            upsert: true,
            update_fields: ['name', 'job']
        };
        const data = [{ some: 'data', name: 'someName', job: 'to be awesome!' }];
        const results = await opTest.processData(opConfig, data);

        expect(results[0]).toEqual({ update: { _index: 'some_index', _type: 'events' } });
        expect(results[1]).toEqual({
            upsert: { some: 'data', name: 'someName', job: 'to be awesome!' },
            doc: { name: 'someName', job: 'to be awesome!' }
        });
    });

    it('script file to run as part of an update request', async () => {
        const opConfig = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events',
            upsert: true,
            update_fields: [],
            script_file: 'someFile',
            script_params: { aKey: 'job' }
        };
        const data = [{ some: 'data', name: 'someName', job: 'to be awesome!' }];
        const results = await opTest.processData(opConfig, data);

        expect(results[0]).toEqual({ update: { _index: 'some_index', _type: 'events' } });
        expect(results[1]).toEqual({
            upsert: { some: 'data', name: 'someName', job: 'to be awesome!' },
            script: { file: 'someFile', params: { aKey: 'to be awesome!' } }
        });
    });

    it('selfValidation makes sure that the opConfig is configured correctly', () => {
        const errorString = 'elasticsearch_index_selector is mis-configured, if any of the following configurations are set: timeseries, index_prefix or date_field, they must all be used together, please set the missing parameters';
        const baseOP = {
            _op: 'elasticsearch_index_selector',
            index: 'some_index',
            type: 'events'
        };

        const op1 = Object.assign({}, baseOP, { timeseries: 'daily' });
        const op2 = Object.assign({}, baseOP, { timeseries: 'daily', index_prefix: 'events-' });
        const op3 = Object.assign({}, baseOP, { timeseries: 'daily', date_field: 'dateField' });
        const op4 = Object.assign({}, baseOP, { timeseries: 'daily', index_prefix: 'events-', date_field: 'dateField' });

        expect(() => {
            indexer.selfValidation(op1);
        }).toThrowError(errorString);
        expect(() => {
            indexer.selfValidation(op2);
        }).toThrowError(errorString);
        expect(() => {
            indexer.selfValidation(op3);
        }).toThrowError(errorString);

        expect(() => {
            indexer.selfValidation(op4);
        }).not.toThrow();
    });
});
