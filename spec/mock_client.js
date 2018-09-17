'use strict';

class MockClient {
    constructor(_sequence) {
        const sequence = [
            {  _shards: { failed: 0 }, hits: { total: 100, hits: [{ _source: { '@timestamp': new Date() } }] } },
            {  _shards: { failed: 0 }, hits: { total: 100, hits: [{ _source: { '@timestamp': new Date() } }] } },
            {  _shards: { failed: 0 }, hits: { total: 100, hits: [{ _source: { '@timestamp': new Date() } }] } },
            {  _shards: { failed: 0 }, hits: { total: 100, hits: [{ _source: { '@timestamp': new Date() } }] } },
            {  _shards: { failed: 0 }, hits: { total: 100, hits: [{ _source: { '@timestamp': new Date() } }] } },
            {  _shards: { failed: 0 }, hits: { total: 100, hits: [{ _source: { '@timestamp': new Date() } }] } }
        ];
        this.sequence = _sequence || sequence;
        this.indices = {};
        this.cluster = {};
        this.deepRecursiveResponseCount = false;

        this.search = async () => {
            const sequence = this.sequence;
            if (sequence.length > 0) {
                return sequence.shift();
            }
            const total = this.deepRecursiveResponseCount || 0;
            return {
                _shards: { failed: 0 },
                hits: { total }
            }
        }

        this.indices.getSettings = async () => {
            return {
                someIndex: {
                    settings: {
                        index: {
                            max_result_window: 10000
                        }
                    }
                }
            };
        }

        this.cluster.stats = async () => {
            return { nodes: { versions: ['2.1.1'] } };
        }

        this.setSequenceData = (data) => {
            this.sequence = data.map(obj => ({ _shards: { failed: 0 }, hits: { total: obj.count !== undefined ? obj.count : 100, hits: [{ _source: obj }] } }));
        }
        
        this.bulk = async (data) => data 
    }
}

 module.exports = MockClient;