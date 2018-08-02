# elasticsearch-assets
bundle of processors for teraslice

# Installation and use

Inside root dir run:
```yarn install```

After root dependencies are installed we now need to load the asset dependencies

```
cd asset
yarn install
```

After everything is installed we now zip the asset folder and send the asset
```
cd ..  //return to root folder
zip -r asset.zip ./asset
curl -XPOST -H "Content-Type: application/octet-stream" TERASLICE_HOSTNAME:5678/assets --data-binary @asset.zip
```

## Operations
 * [processor documentation](./docs/ops_reference.md)