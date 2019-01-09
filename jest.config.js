'use strict';

module.exports = {
    rootDir: '.',
    verbose: true,
    testMatch: [
        '<rootDir>/spec/*-spec.js'
    ],
    collectCoverage: true,
    collectCoverageFrom: [
        '<rootDir>/asset/**/*.js',
        '<rootDir>/asset/*/*.js',
        '!<rootDir>/asset/node_modules',
    ],
    coverageReporters: ['lcov', 'text', 'html'],
    coverageDirectory: '<rootDir>/coverage'
};
