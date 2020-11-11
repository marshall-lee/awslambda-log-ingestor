const webpack = require('webpack');
const path = require('path');

module.exports = {
  target: 'node',
  output: {
    path: path.resolve(__dirname, 'extensions'),
    filename: 'log-ingestor',
  },
  plugins: [
    new webpack.BannerPlugin({ banner: '#!/usr/bin/env node', raw: true }),
  ]
};

