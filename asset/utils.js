'use strict';

const parseError = require('@terascope/error-parser');

function dateOptions(value) {
    const options = {
        year: 'y',
        years: 'y',
        y: 'y',
        months: 'M',
        month: 'M',
        mo: 'M',
        mos: 'M',
        M: 'M',
        weeks: 'w',
        week: 'w',
        wks: 'w',
        wk: 'w',
        w: 'w',
        days: 'd',
        day: 'd',
        d: 'd',
        hours: 'h',
        hour: 'h',
        hr: 'h',
        hrs: 'h',
        h: 'h',
        minutes: 'm',
        minute: 'm',
        min: 'm',
        mins: 'm',
        m: 'm',
        seconds: 's',
        second: 's',
        s: 's',
        milliseconds: 'ms',
        millisecond: 'ms',
        ms: 'ms'
    };

    if (options[value]) {
        return options[value];
    }

    throw new Error('the time descriptor for the interval is malformed');
}


// "2016-01-19T13:33:09.356-07:00"
const dateFormat = 'YYYY-MM-DDTHH:mm:ss.SSSZ';

// 2016-06-29T12:44:57-07:00
const dateFormatSeconds = 'YYYY-MM-DDTHH:mm:ssZ';

function retryModule(logger, numOfRetries) {
    const retry = {};
    return (key, err, fn, msg) => {
        const errMessage = parseError(err);
        logger.error('error while getting next slice', errMessage);

        if (!retry[key]) {
            retry[key] = 1;
            return fn(msg);
        }

        retry[key] += 1;
        if (retry[key] > numOfRetries) {
            return Promise.reject(`max_retries met for slice, key: ${key}`, errMessage);
        }

        return fn(msg);
    };
}

// FIXME: this function is duplication of apis. Putting this in for 
// backwards compatibility until that api is settled
function getOpConfig(job, name) {
    return job.operations.find(op => op._op === name);
}

// FIXME: this function is duplication of apis. Putting this in for
// backwards compatibility until that api is settled
function getClient(context, config, type) {
    const clientConfig = {};
    const events = context.foundation.getEventEmitter();
    clientConfig.type = type;

    if (config && 'connection' in config) {
        clientConfig.endpoint = config.connection ? config.connection : 'default';
        clientConfig.cached = config.connection_cache !== undefined ? config.connection_cache : true;
    } else {
        clientConfig.endpoint = 'default';
        clientConfig.cached = true;
    }
    try {
        return context.foundation.getConnection(clientConfig).client;
    } catch (err) {
        const errMsg = `No configuration for endpoint ${clientConfig.endpoint} was found in the terafoundation connectors config, error: ${err.stack}`;
        context.logger.error(errMsg);
        events.emit('client:initialization:error', { error: errMsg });
    }
}

module.exports = {
    dateOptions,
    dateFormat,
    dateFormatSeconds,
    retryModule,
    getOpConfig,
    getClient
};
