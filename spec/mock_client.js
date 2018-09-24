'use strict';

class MockClient {
    constructor(_sequence) {
        const defaultSequence = [
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } },
            { _shards: { failed: 0 }, hits: { total: 100, hits: [{ _id: 'someId', _source: { '@timestamp': new Date() } }] } }
        ];
        this.sequence = _sequence || defaultSequence;
        this.indices = {};
        this.cluster = {};
        this.deepRecursiveResponseCount = false;

        this.search = async () => {
            const { sequence } = this;
            if (sequence.length > 0) {
                return sequence.shift();
            }
            const total = this.deepRecursiveResponseCount || 0;
            return {
                _shards: { failed: 0 },
                hits: { total }
            };
        };

        this.indices.getSettings = async () => {
            const window = 10000;
            return {
                someIndex: {
                    settings: {
                        index: {
                            max_result_window: window
                        }
                    }
                }
            };
        };

        this.cluster.stats = async () => {
            const defaultVersion = '2.1.1';
            return { nodes: { versions: [defaultVersion] } };
        };

        this.setSequenceData = (data) => {
            this.sequence = data.map(
                obj => ({
                    _shards: { failed: 0 },
                    hits: {
                        total: obj.count !== undefined ? obj.count : 100,
                        hits: [{ _source: obj }]
                    }
                })
            );
        };

        this.bulk = async data => data;
    }
}

module.exports = MockClient;
