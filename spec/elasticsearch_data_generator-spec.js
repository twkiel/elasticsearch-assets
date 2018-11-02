'use strict';

const Promise = require('bluebird');
const harness = require('@terascope/teraslice-op-test-harness');
const generator = require('../asset/elasticsearch_data_generator');

describe('elasticsearch_data_generator', () => {
    const opTest = harness(generator);

    it('has a schema and newReader method', () => {
        expect(generator).toBeDefined();
        expect(generator.newReader).toBeDefined();
        expect(generator.newSlicer).toBeDefined();
        expect(generator.schema).toBeDefined();
        expect(typeof generator.newReader).toEqual('function');
        expect(typeof generator.newSlicer).toEqual('function');
        expect(typeof generator.schema).toEqual('function');
    });

    it('newReader returns a function that produces generated data', async () => {
        const opConfig = { _op: 'elasticsearch_data_generator' };

        const test = await opTest.init({ opConfig, type: 'reader' });

        const results = await test.run(1);
        expect(results.length).toEqual(1);
        expect(Object.keys(results[0]).length).toBeGreaterThan(1);
    });

    it('slicer in "once" mode will return number based off total size ', async () => {
        const executionConfig1 = {
            lifecycle: 'once',
            operations: [
                { _op: 'elasticsearch_data_generator', size: 15 },
                { _op: 'noop', size: 5 }
            ]
        };

        // if not specified size defaults to 5000
        const executionConfig2 = {
            lifecycle: 'once',
            operations: [
                { _op: 'elasticsearch_data_generator', someKey: 'someValue', size: 13 },
                { _op: 'noop', size: 5000 }
            ]
        };


        const test = await opTest.init({ executionConfig: executionConfig1 });
        const test2 = await opTest.init({ executionConfig: executionConfig2 });

        const [s1Results1, s2Results1] = await Promise.all([test.run(), test2.run()]);
        expect(s1Results1).toEqual([5]);
        expect(s2Results1).toEqual([13]);

        const [s1Results2, s2Results2] = await Promise.all([test.run(), test2.run()]);
        expect(s1Results2).toEqual([5]);
        expect(s2Results2).toEqual(null);

        const [s1Results3, s2Results3] = await Promise.all([test.run(), test2.run()]);
        expect(s1Results3).toEqual([5]);
        expect(s2Results3).toEqual(null);

        const [s1Results4, s2Results4] = await Promise.all([test.run(), test2.run()]);
        expect(s1Results4).toEqual(null);
        expect(s2Results4).toEqual(null);
    });

    it('slicer in "persistent" mode will continuously produce the same number', async () => {
        const executionConfig = { lifecycle: 'persistent', slicers: 3, operations: [{ _op: 'elasticsearch_data_generator', size: 550 }] };

        const test = await opTest.init({ executionConfig });
        const results1 = await test.run();
        const results2 = await test.run();
        const results3 = await test.run();

        expect(results1).toEqual([550]);
        expect(results2).toEqual([550]);
        expect(results3).toEqual([550]);
    });

    it('data generator will only return one slicer', async () => {
        const executionConfig = { lifecycle: 'persistent', slicers: 3, operations: [{ _op: 'elasticsearch_data_generator', size: 5 }] };

        const test = await opTest.init({ executionConfig });
        expect(test.operation.slicers()).toEqual(1);
    });
});
