'use strict';

const processor = require('../asset/simple_api_reader');

class Events {
    on() {
        return this;
    }
}

class Stream {
    on() {
        return this;
    }

    resume() {
        return this;
    }
}

const context = {
    __test_mocks: {
        get: (request, cb) => {
            console.log(request)
            cb(new Stream())

            return new Events();
        }
    }
};

const opConfig = {
    _op: 'simple_api_reader',
    index: 'details-subset',
    endpoint: 'https://localhost:8000',
    token: 'test-token',
    size: 100000,
    interval: '30s',
    delay: '30s',
    date_field_name: 'date'
};

describe('simple_api_reader', () => {
    it('should look like an elasticsearch client', () => {
        const client = processor.createClient(context, opConfig);

        expect(client.search).toBeDefined();
        expect(client.count).toBeDefined();
        expect(client.cluster).toBeDefined();
        expect(client.cluster.stats).toBeDefined();
        expect(client.cluster.getSettings).toBeDefined();
    });

    it('search should generate url', () => {
        const client = processor.createClient(context, opConfig);

        // TODO: this isn't fleshed out yet
        client.search({
            q: '',
            body: {
                query: {
                    bool: {
                        must: [
                            {
                                range: {
                                    date: {
                                        gte: '2017-09-23T18:07:14.332Z',
                                        lt: '2017-09-25T18:07:14.332Z'
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        });

        client.search({
            q: '',
            size: 100,
            body: {
                query: {
                    bool: {
                        must: [
                            {
                                query_string: {
                                    query: "test:query"
                                }
                            },
                            {
                                range: {
                                    date: {
                                        gte: '2017-09-23T18:07:14.332Z',
                                        lt: '2017-09-25T18:07:14.332Z'
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        });

        client.search({
            q: 'test:query',
            size: 100,
            body: {
                sort: {}
            }
        });
    });
});
