'use strict';

require('jest-extended');
const path = require('path');
const { WorkerTestHarness, newTestJobConfig } = require('teraslice-test-harness');
const { DataEntity } = require('@terascope/job-components');

describe('elasticsearch state storage api', () => {
    const idField = '_key';

    class TestClient {
        setGetData(data) {
            this.getData = data;
        }

        setMGetData(data) {
            this.mgetData = data;
        }

        async get() {
            return this.getData;
        }

        async mget() {
            return this.mgetData;
        }

        async bulk(request) {
            this.bulkRequest = request.body;
            return request;
        }
    }

    const client = new TestClient();

    function addTestMeta(obj, index) {
        return DataEntity.make(obj, { [idField]: index + 1 });
    }

    const docArray = [
        {
            data: 'thisIsFirstData'
        },
        {
            data: 'thisIsSecondData'
        },
        {
            data: 'thisIsThirdData'
        }
    ];

    const clientConfig = {
        type: 'elasticsearch',
        create() {
            return { client };
        }
    };

    const job = newTestJobConfig({
        max_retries: 3,
        apis: [
            {
                _name: 'elasticsearch_state_storage:foo',
                index: 'someIndex',
                type: 'type',
                id_field: idField
            },
            {
                _name: 'elasticsearch_state_storage:bar',
                index: 'someIndex',
                type: 'type',
                id_field: idField
            }
        ],
        operations: [
            {
                _op: 'test-reader',
                passthrough_slice: true
            },
            {
                _op: 'noop',
                state_storage_api: 'elasticsearch_state_storage:foo'
            },
            {
                _op: 'noop',
                state_storage_api: 'elasticsearch_state_storage:bar'
            }
        ],
    });

    let harness;
    let noopFoo;
    let noopBar;
    let countFoo;
    let countBar;

    beforeEach(async () => {
        harness = new WorkerTestHarness(job, {
            assetDir: path.join(__dirname, '../asset'),
            clients: [clientConfig],
        });

        noopFoo = harness.getOperation(1);
        noopBar = harness.getOperation(2);
        const reader = harness.getOperation('test-reader');
        const fn = reader.fetch.bind(reader);
        // NOTE: we do not have a good story around added meta data to testing data
        reader.fetch = async incDocs => fn(incDocs.map(addTestMeta));

        noopFoo.onBatch = async (docs) => {
            const results = [];
            const { state_storage_api: name } = noopFoo.opConfig;
            const stateStorage = noopFoo.getAPI(name);
            await stateStorage.mset(docs);
            countFoo = stateStorage.count();
            const cached = await stateStorage.mget(docs);

            // eslint-disable-next-line guard-for-in
            for (const key in cached) {
                results.push(cached[key]);
            }
            return results;
        };

        noopBar.onBatch = async (docs) => {
            const { state_storage_api: name } = noopBar.opConfig;
            const stateStorage = noopFoo.getAPI(name);
            countBar = stateStorage.count();
            return docs;
        };

        await harness.initialize();
    });

    afterEach(async () => {
        await harness.shutdown();
    });

    it('can run and use the api', async () => {
        const results = await harness.runSlice(docArray);

        expect(countFoo).toEqual(3);
        expect(countBar).toEqual(0);
        expect(results.length).toEqual(3);

        results.forEach((obj, ind) => {
            expect(obj).toEqual(docArray[ind]);
            expect(DataEntity.isDataEntity(obj)).toEqual(true);
        });
    });
});
