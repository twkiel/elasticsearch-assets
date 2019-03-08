'use strict';

module.exports = {
    rootDir: '.',
    verbose: true,
    setupFilesAfterEnv: [
        'jest-extended'
    ],
    testMatch: [
        '<rootDir>/test/*-spec.js'
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
