'use strict';

const Promise = require('bluebird');
const processor = require('../asset/simple_api_reader');

function makeQuery(opConfig, msg, data, sort) {
    const query = {
        index: opConfig.index,
        size: msg.count,
        body: {
            query: {
                bool: {
                    must: Array.isArray(data) ? data : [data]
                }
            }
        }
    };
    if (opConfig.fields) {
        query._source = opConfig.fields;
    }
    if (sort) query.body.sort = [sort];
    return query;
}

describe('simple_api_reader', () => {
    let allRequests = [];
    let timeout = null;

    beforeEach(() => {
        timeout = null;
    })

    const data = {
        results: [{ _source: { some: 'data' } }],
        total: 1
    };

    const opConfig = {
        _op: 'simple_api_reader',
        index: 'details-subset',
        endpoint: 'https://localhost:8000',
        token: 'test-token',
        size: 100000,
        interval: '30s',
        delay: '30s',
        date_field_name: 'date',
        timeout: 50
    };

    const opConfig2 = {
        _op: 'simple_api_reader',
        index: 'details-subset',
        endpoint: 'https://localhost:8000',
        token: 'test-token',
        size: 100000,
        interval: '30s',
        delay: '30s',
        date_field_name: 'date',
        geo_field: 'some_field',
        geo_box_top_left: '34.5234,79.42345',
        geo_box_bottom_right: '54.5234,80.3456',
        geo_sort_point: '52.3456,79.6784',
        timeout: 50
    };

    const opConfig3 = {
        _op: 'simple_api_reader',
        index: 'details-subset',
        endpoint: 'https://localhost:8000',
        token: 'test-token',
        size: 100000,
        interval: '30s',
        delay: '30s',
        date_field_name: 'date',
        geo_distance: '200km',
        geo_point: '52.3456,79.6784',
        timeout: 50
    };

    const opConfig4 = {
        _op: 'simple_api_reader',
        index: 'details-subset',
        endpoint: 'https://localhost:8000',
        token: 'test-token',
        size: 5000,
        interval: '90d',
        date_field_name: 'date',
        query: 'bytes:>=2000',
        timeout: 50
    };

    const context = {
        __test_mocks: (query) => {
            allRequests.push(query.uri);
            return new Promise((resolve) => {
                if (!timeout) resolve(data);
            
                setTimeout(() => {
                    resolve(data)
                }, timeout);
            })
        },
        logger: {
            error: () => {},
            info: () => {},
            warn: () => {},
            debug: () => {},
            trace: () => {}
        }
    };

    beforeEach(() => {
        allRequests = [];
    });

    it('should look like an elasticsearch client', () => {
        const client = processor.createClient(context, opConfig);

        expect(client.search).toBeDefined();
        expect(client.count).toBeDefined();
        expect(client.cluster).toBeDefined();
        expect(client.cluster.stats).toBeDefined();
        expect(client.cluster.getSettings).toBeDefined();
    });

    it('search should generate url', (done) => {
        const client = processor.createClient(context, opConfig);
        const client2 = processor.createClient(context, opConfig2);
        const client3 = processor.createClient(context, opConfig3);
        const client4 = processor.createClient(context, opConfig4);

        const rangeQuery = {
            range: {
                date: {
                    gte: '2017-09-23T18:07:14.332Z',
                    lt: '2017-09-25T18:07:14.332Z'
                }
            }
        };
        const luceneQuery = {
            query_string: {
                query: 'test:query'
            }
        };

        const geoQuery = {
            geo_bounding_box: {
                some_field: {
                    top_left: {
                        lat: '34.5234',
                        lon: '79.42345'
                    },
                    bottom_right: {
                        lat: '54.5234',
                        lon: '80.3456'
                    }
                }
            }
        };

        const sort1 = {
            _geo_distance: {
                some_field: {
                    lat: '52.3456',
                    lon: '79.6784'
                },
                order: 'asc',
                unit: 'm'
            }
        };

        const sort2 = {
            "date": {
                "order":"asc"
            }
        };

        const queryOpConfig = Object.assign({}, opConfig, { query: 'test:query' });
        const geoOpConfig = Object.assign({}, opConfig2, { query: 'test:query' });

        const query1 = makeQuery(opConfig, { count: 100 }, rangeQuery);
        const query2 = makeQuery(queryOpConfig, { count: 100 }, [luceneQuery, rangeQuery]);
        const query3 = makeQuery(geoOpConfig, { count: 100 }, [luceneQuery, rangeQuery, geoQuery], sort1);
        const query4 = makeQuery(geoOpConfig, { count: 100 }, [luceneQuery, rangeQuery, geoQuery]);
        const query5 = makeQuery(geoOpConfig, { count: 100 }, [luceneQuery, rangeQuery, geoQuery], sort2);
        const query6 = {"index":"details-subset","size":5000,"body":{"sort":[{"created":{"order":"asc"}}]},"q":"bytes:>=2000"};

        const url1 = 'https://localhost:8000/details-subset?token=test-token&q=date:[2017-09-23T18:07:14.332Z TO 2017-09-25T18:07:14.332Z}&size=100';
        const url2 = 'https://localhost:8000/details-subset?token=test-token&q=test:query AND date:[2017-09-23T18:07:14.332Z TO 2017-09-25T18:07:14.332Z}&size=100';
        const url3 ='https://localhost:8000/details-subset?token=test-token&q=test:query AND date:[2017-09-23T18:07:14.332Z TO 2017-09-25T18:07:14.332Z}&size=100&geo_box_top_left=34.5234,79.42345&geo_box_bottom_right=54.5234,80.3456&geo_sort_point=52.3456,79.6784';
        const url4 = 'https://localhost:8000/details-subset?token=test-token&q=test:query AND date:[2017-09-23T18:07:14.332Z TO 2017-09-25T18:07:14.332Z}&size=100&geo_point=52.3456,79.6784&geo_distance=200km';
        const url5 = 'https://localhost:8000/details-subset?token=test-token&q=test:query AND date:[2017-09-23T18:07:14.332Z TO 2017-09-25T18:07:14.332Z}&size=100&sort=date:asc&geo_point=52.3456,79.6784&geo_distance=200km';
        const url6 = 'https://localhost:8000/details-subset?token=test-token&q=bytes:>=2000&size=5000';

        Promise.resolve()
            .then(() => client.search(query1))
            .then(() => {
                const url = allRequests.pop();
                expect(url).toEqual(url1);
                return client.search(query2);
            })
            .then(() => {
                const url = allRequests.pop();
                expect(url).toEqual(url2);
                return client2.search(query3);
            })
            .then(() => {
                const url = allRequests.pop();
                expect(url).toEqual(url3);
                return client3.search(query4);
            })
            .then(() => {
                const url = allRequests.pop();
                expect(url).toEqual(url4);
                return client3.search(query5);
            })
            .then(() => {
                const url = allRequests.pop();
                expect(url).toEqual(url5);
                return client4.search(query6)
            })
            .then(() => {
                const url = allRequests.pop();
                expect(url).toEqual(url6);
            })
            .catch(fail)
            .finally(done);
    });

    it('request can timeout properly', (done) => {
        const client = processor.createClient(context, opConfig);
        timeout = 75;

        const rangeQuery = {
            range: {
                date: {
                    gte: '2017-09-23T18:07:14.332Z',
                    lt: '2017-09-25T18:07:14.332Z'
                }
            }
        };
        const query1 = makeQuery(opConfig, { count: 100 }, rangeQuery);

        Promise.resolve()
            .then(() => client.search(query1))
            .catch(err => expect(err).toEqual('HTTP request timed out connecting to API endpoint.'))
            .finally(done);
    });
});
