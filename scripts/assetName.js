'use strict';

const asset = require('../asset/asset');

console.log(`${asset.name}-${asset.version}-${process.platform}-${process.arch}.zip`);
