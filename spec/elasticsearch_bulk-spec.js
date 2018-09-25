'use strict';

const harness = require('@terascope/teraslice-op-test-harness');
const esSender = require('../asset/elasticsearch_bulk');
const MockClient = require('./mock_client');


describe('elasticsearch_bulk', () => {
    const opTest = harness(esSender);
    let client;

    beforeEach(() => {
        client = new MockClient();
        opTest.setClients([{ client, type: 'elasticsearch' }]);
    });

    it('has both a newSender and schema method', () => {
        expect(esSender.newProcessor).toBeDefined();
        expect(esSender.schema).toBeDefined();
        expect(typeof esSender.newProcessor).toEqual('function');
        expect(typeof esSender.schema).toEqual('function');
    });

    it('schema has defaults', () => {
        const defaults = esSender.schema();

        expect(defaults.size).toBeDefined();
        expect(defaults.size.default).toEqual(500);
    });

    it('returns a function', async () => {
        const opConfig = { _op: 'elasticsearch_bulk', size: 100, multisend: false };
        const test = await opTest.init(opConfig);
        expect(typeof test.operation).toEqual('function');
    });

    it('if no docs, returns a promise of passed in data', async () => {
        const opConfig = { _op: 'elasticsearch_bulk', size: 100, multisend: false };
        const test = await opTest.init({ opConfig });
        const results = await test.run([]);

        expect(results).toEqual([]);
    });

    it('does not split if the size is <= than 2 * size in opConfig', async () => {
        // usually each doc is paired with metadata, thus doubling the size of incoming array,
        // hence we double size
        const opConfig = { _op: 'elasticsearch_bulk', size: 50, multisend: false };
        const incData = [];

        for (let i = 0; i < 50; i += 1) {
            incData.push({ index: 'some_index' }, { some: 'data' });
        }

        const test = await opTest.init({ opConfig });
        const results = await test.run(incData);

        expect(results.length).toEqual(1);
        expect(results[0].body.length).toEqual(100);
    });

    it('it does split if the size is greater than 2 * size in opConfig', async () => {
        // usually each doc is paired with metadata, thus doubling the size of incoming array,
        // hence we double size
        const opConfig = { _op: 'elasticsearch_bulk', size: 50, multisend: false };
        const incData = [];

        for (let i = 0; i < 120; i += 1) {
            incData.push({ some: 'data' });
        }

        const test = await opTest.init({ opConfig });
        const results = await test.run(incData);

        expect(results.length).toEqual(2);
        expect(results[0].body.length).toEqual(101);
        expect(results[1].body.length).toEqual(19);
    });

    it('it splits the array up properly when there are delete operations (not a typical doubling of data)', async () => {
        const opConfig = { _op: 'elasticsearch_bulk', size: 2, multisend: false };
        const incData = [{ create: {} }, { some: 'data' }, { update: {} }, { other: 'data' }, { delete: {} }, { index: {} }, { final: 'data' }];
        const copy = incData.slice();

        const test = await opTest.init({ opConfig });
        const results = await test.run(incData);

        expect(results.length).toEqual(2);
        expect(results[0].body).toEqual(copy.slice(0, 5));
        expect(results[1].body).toEqual(copy.slice(5));
    });

    it('multisend will send based off of _id ', async () => {
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            connection_map: {
                a: 'default'
            }
        };

        const incData = [{ create: { _id: 'abc' } }, { some: 'data' }, { update: { _id: 'abc' } }, { other: 'data' }, { delete: { _id: 'abc' } }, { index: { _id: 'abc' } }, { final: 'data' }];
        const copy = incData.slice();

        const test = await opTest.init({ opConfig });
        const results = await test.run(incData);

        expect(results.length).toEqual(1);
        // length to index is off by 1
        expect(results[0].body).toEqual(copy);
    });

    it('it can multisend to several places', async () => {
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            connection_map: {
                a: 'default',
                b: 'otherConnection'
            }
        };
        const client1 = new MockClient();
        const client2 = new MockClient();
        opTest.setClients([
            { client: client1, type: 'elasticsearch', endpoint: 'default' },
            { client: client2, type: 'elasticsearch', endpoint: 'otherConnection' }
        ]);

        const incData = [{ create: { _id: 'abc' } }, { some: 'data' }, { update: { _id: 'abc' } }, { other: 'data' }, { delete: { _id: 'bc' } }, { index: { _id: 'bc' } }, { final: 'data' }];
        const copy = incData.slice();

        const test = await opTest.init({ opConfig });
        const results = await test.run(incData);

        expect(results.length).toEqual(2);
        // length to index is off by 1
        expect(results[0].body).toEqual(copy.slice(0, 4));
        expect(results[1].body).toEqual(copy.slice(4));
    });

    it('multisend_index_append will change outgoing _id ', async () => {
        const opConfig = {
            _op: 'elasticsearch_bulk',
            size: 5,
            multisend: true,
            multisend_index_append: 'hello',
            connection_map: {
                a: 'default'
            }
        };
        const incData = [{ create: { _id: 'abc' } }, { some: 'data' }, { update: { _id: 'abc' } }, { other: 'data' }, { delete: { _id: 'abc' } }, { index: { _id: 'abc' } }, { final: 'data' }];
        const copy = incData.slice();

        const test = await opTest.init({ opConfig });
        const results = await test.run(incData);

        expect(results.length).toEqual(1);
        // length to index is off by 1
        expect(results[0].body).toEqual(copy);
    });

    it('crossValidation makes sure connection_map is configured in sysconfig', () => {
        const badJob = {
            operations: [{
                _op: 'elasticsearch_bulk',
                multisend: true,
                connection_map: { a: 'connectionA', z: 'connectionZ' }
            }]
        };
        const goodJob = {
            operations: [{
                _op: 'elasticsearch_bulk',
                multisend: true,
                connection_map: { a: 'connectionA', b: 'connectionB' }
            }]
        };

        const sysconfig = {
            terafoundation: {
                connectors: {
                    elasticsearch: {
                        connectionA: 'connection Config',
                        connectionB: 'otherConnection Config'
                    }
                }
            }
        };
        const errorString = 'elasticsearch_bulk connection_map specifies a connection for [connectionZ] but is not found in the system configuration [terafoundation.connectors.elasticsearch]';

        expect(() => {
            esSender.crossValidation(badJob, sysconfig);
        }).toThrowError(errorString);

        expect(() => {
            esSender.crossValidation(goodJob, sysconfig);
        }).not.toThrow();
    });
});
