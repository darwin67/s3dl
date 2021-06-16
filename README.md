# S3 file downloader

#### Why not just use the `awscli` tool?

The awscli tool surprisingly doesn't have good support for downloading files
with non-ascii filenames.

[Filename encoding errors](https://github.com/aws/aws-cli/issues/1368)

In this issue, some people seem to got it working with unicode but I somehow
can't get it working, so I wrote this little script to handle it for me.
