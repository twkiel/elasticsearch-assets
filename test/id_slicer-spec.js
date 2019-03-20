'use strict';

const harness = require('@terascope/teraslice-op-test-harness');
const MockClient = require('./mock_client');
const idReader = require('../asset/id_reader');

describe('id_reader', () => {
    const opTest = harness(idReader);
    let client;

    beforeEach(() => {
        client = new MockClient();
        opTest.setClients([{ client, type: 'elasticsearch' }]);
    });

    it('has a schema, newSlicer and a newReader method, crossValidation', () => {
        const reader = idReader;

        expect(reader).toBeDefined();
        expect(reader.newSlicer).toBeDefined();
        expect(reader.schema).toBeDefined();
        expect(reader.newReader).toBeDefined();
        expect(reader.crossValidation).toBeDefined();

        expect(typeof reader.newSlicer).toEqual('function');
        expect(typeof reader.newReader).toEqual('function');
        expect(typeof reader.schema).toEqual('function');
        expect(typeof reader.crossValidation).toEqual('function');
    });

    it('crossValidation makes sure its configured correctly', () => {
        const errorStr1 = 'The number of slicers specified on the job cannot be more the length of key_range';
        const errorStr2 = 'The number of slicers specified on the job cannot be more than 16';
        const errorStr3 = 'The number of slicers specified on the job cannot be more than 64';

        const job1 = { slicers: 1, operations: [{ _op: 'id_reader', key_range: ['a', 'b'] }] };
        const job2 = { slicers: 2, operations: [{ _op: 'id_reader', key_range: ['a'] }] };
        const job3 = { slicers: 4, operations: [{ _op: 'id_reader', key_type: 'hexadecimal' }] };
        const job4 = { slicers: 20, operations: [{ _op: 'id_reader', key_type: 'hexadecimal' }] };
        const job5 = { slicers: 20, operations: [{ _op: 'id_reader', key_type: 'base64url' }] };
        const job6 = { slicers: 70, operations: [{ _op: 'id_reader', key_type: 'base64url' }] };

        function testValidation(job) {
            idReader.crossValidation(job);
        }
        expect(() => {
            testValidation(job1);
        }).not.toThrow();
        expect(() => {
            testValidation(job2);
        }).toThrowError(errorStr1);

        expect(() => {
            testValidation(job3);
        }).not.toThrow();
        expect(() => {
            testValidation(job4);
        }).toThrowError(errorStr2);

        expect(() => {
            testValidation(job5);
        }).not.toThrow();
        expect(() => {
            testValidation(job6);
        }).toThrowError(errorStr3);
    });

    it('can create multiple slicers', async () => {
        const executionConfig1 = {
            slicers: 1,
            operations: [{
                _op: 'id_reader',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                type: 'events-',
                index: 'someindex'
            }]
        };
        const executionConfig2 = {
            slicers: 2,
            operations: [{
                _op: 'id_reader',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                type: 'events-',
                index: 'someindex'
            }]
        };

        const singleSlicer = await opTest.init({ executionConfig: executionConfig1 });

        expect(singleSlicer.operation.slicers()).toEqual(1);

        const multiSlicers = await opTest.init({ executionConfig: executionConfig2 });

        expect(multiSlicers.operation.slicers()).toEqual(2);
    });

    it('it produces values', async () => {
        const executionConfig = {
            slicers: 1,
            operations: [{
                _op: 'id_reader',
                type: 'events-',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            }]
        };
        const test = await opTest.init({ executionConfig });

        const [slice1] = await test.run();
        expect(slice1).toEqual({ count: 100, key: 'events-#a*' });

        const [slice2] = await test.run();
        expect(slice2).toEqual({ count: 100, key: 'events-#b*' });

        const slice3 = await test.run();
        expect(slice3).toEqual([null]);
    });

    it('it produces values starting at a specific depth', async () => {
        const executionConfig = {
            slicers: 1,
            operations: [{
                _op: 'id_reader',
                type: 'events-',
                key_type: 'hexadecimal',
                key_range: ['a', 'b', 'c', 'd'],
                starting_key_depth: 3,
                index: 'someindex',
                size: 200
            }]
        };
        const test = await opTest.init({ executionConfig });

        const [slice1] = await test.run();
        expect(slice1).toEqual({ count: 100, key: 'events-#a00*' });

        const [slice2] = await test.run();
        expect(slice2).toEqual({ count: 100, key: 'events-#a01*' });

        const [slice3] = await test.run();
        expect(slice3).toEqual({ count: 100, key: 'events-#a02*' });
    });

    it('it produces values even with an initial search error', async () => {
        const executionConfig = {
            slicers: 1,
            operations: [{
                _op: 'id_reader',
                type: 'events-',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            }]
        };
        const { sequence } = client;

        sequence.pop();
        client.sequence = [{
            _shards: {
                failed: 1,
                failures: [{ reason: { type: 'some Error' } }]
            }
        },
        ...sequence
        ];

        const test = await opTest.init({ executionConfig });

        const [slice1] = await test.run();
        expect(slice1).toEqual({ count: 100, key: 'events-#a*' });

        const [slice2] = await test.run();
        expect(slice2).toEqual({ count: 100, key: 'events-#b*' });

        const slice3 = await test.run();
        expect(slice3).toEqual([null]);
    });

    it('key range gets divided up by number of slicers', async () => {
        const executionConfig = {
            slicers: 2,
            operations: [{
                _op: 'id_reader',
                type: 'events-',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            }]
        };

        const test = await opTest.init({ executionConfig });

        const slices1 = await test.run();
        expect(slices1[0]).toEqual({ count: 100, key: 'events-#a*' });
        expect(slices1[1]).toEqual({ count: 100, key: 'events-#b*' });

        const slices2 = await test.run();

        expect(slices2).toEqual([null, null]);
    });

    it('key range gets divided up by number of slicers', async () => {
        const newSequence = [
            { _shards: { failed: 0 }, hits: { total: 100 } },
            { _shards: { failed: 0 }, hits: { total: 500 } },
            { _shards: { failed: 0 }, hits: { total: 200 } },
            { _shards: { failed: 0 }, hits: { total: 200 } },
            { _shards: { failed: 0 }, hits: { total: 100 } }
        ];

        const executionConfig = {
            slicers: 1,
            operations: [{
                _op: 'id_reader',
                type: 'events-',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            }]
        };

        client.sequence = newSequence;
        const test = await opTest.init({ executionConfig });

        const [slice1] = await test.run();
        expect(slice1).toEqual({ count: 100, key: 'events-#a*' });

        const [slice2] = await test.run();
        expect(slice2).toEqual({ count: 200, key: 'events-#b0*' });

        const [slice3] = await test.run();
        expect(slice3).toEqual({ count: 200, key: 'events-#b1*' });

        const [slice4] = await test.run();
        expect(slice4).toEqual({ count: 100, key: 'events-#b2*' });

        const slice5 = await test.run();
        expect(slice5).toEqual([null]);
    });

    it('can return to previous position', async () => {
        const retryData = [{ lastSlice: { key: 'events-#a6*' } }];
        const executionConfig = {
            slicers: 1,
            operations: [{
                _op: 'id_reader',
                type: 'events-',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            }]
        };
        const test = await opTest.init({ executionConfig, retryData });

        const [slice1] = await test.run();
        expect(slice1).toEqual({ count: 100, key: 'events-#a7*' });

        const [slice2] = await test.run();
        expect(slice2).toEqual({ count: 100, key: 'events-#a8*' });

        const [slice3] = await test.run();
        expect(slice3).toEqual({ count: 100, key: 'events-#a9*' });

        const [slice4] = await test.run();
        expect(slice4).toEqual({ count: 100, key: 'events-#aa*' });

        const [slice5] = await test.run();
        expect(slice5).toEqual({ count: 100, key: 'events-#ab*' });
    });

    it('newReader returns a function that queries elasticsearch', async () => {
        const executionConfig = {
            lifecycle: 'once',
            operations: [{
                _op: 'id_reader',
                type: 'events-',
                key_type: 'hexadecimal',
                key_range: ['a', 'b'],
                index: 'someindex',
                size: 200
            }]
        };

        const type = 'reader';

        const reader = await opTest.init({ executionConfig, type });
        expect(typeof reader.operation).toEqual('object');
    });
});
