#!/usr/bin/env bash

set -e -u

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
SRCDIR="${DIR}/.."

# Make sure the output dir exists
OUTDIR=${SRCDIR}/builds
mkdir -p $OUTDIR

# Generate the asset name
ASSETNAME=$(node ${DIR}/assetName.js)
#echo "Output asset to be named: $ASSETNAME"

# Create the tempdir
TMPDIR=`mktemp -d`
echo $TMPDIR

# Copy the source to the tmpdir
cp -a ${SRCDIR} ${TMPDIR}

# Remove any old node_modules to make sure they get updated
rm -rf ${TMPDIR}/asset/node_modules

# Build and install asset dependencies
cd ${TMPDIR}/asset
yarn --prod

# Zip up generated asset directory
cd ..
zip -r $OUTDIR/$ASSETNAME asset
ls $(realpath $OUTDIR/$ASSETNAME)

# Cleanup
rm -rf $TMPDIR