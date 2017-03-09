# vg-logger
A configuration of nxlog-ce to support processing video game logs. Versioned
using [semantic versioning](http://semver.org/)

I chose nxlog because it's cross-platform, has the features needed, it's free
(as in beer), and the source code is available for those that wish to look at
it.

This repo only supports Windows 7 or later. Linux support requires a bunch of
paths in the config files to be changed.

# Games supported

  - *The Secret World* logging to a mix of CouchDB and ElasticSearch.

# Installation instructions

  1. Install [nxlog](http://nxlog.co/products/nxlog-community-edition/download).
  2. Replace nxlog's default config with `$REPO/nxlog/nxlog.conf`
  3. Create `C:\ProgramData\nxlog\conf` and `C:\ProgramData\nxlog\data`
  4. Copy the contents of `$REPO/nxlog/conf/` into `C:\ProgramData\nxlog\conf\`
