# site-analyser

A simple Clojure site analyser designed for websites with CloudFront logs.

## Development

If you are using Nix, you can start a shell with all of the dependencies needed
for development like so:

``` sh
nix-shell
```

OK, that was a little bit of a lie, as you'll also need to clone the
[blambda](https://github.com/jmglov/blambda) repo:

``` sh
git clone https://github.com/jmglov/blambda.git
```

If you're not using Nix, you'll need to install
[Babashka](https://babashka.org/) and [Terraform](https://www.terraform.io/) (if
using it for managing your infrastructure) in the manner of your choosing.

## Building

Set the following environment variables:
- `ARTIFACTS_BUCKET`: S3 bucket to use for lambda artifacts
- `CLOUDFRONT_DIST_ID`: ID of your Cloudfront distribution
- `S3_BUCKET`: S3 bucket where your logs are stored
- `S3_PREFIX`: prefix in the bucket where your logs are stored

Run:

``` sh
bb blambda build-all
```

## Deploying

Run:

``` sh
bb blambda terraform write-config
bb blambda terraform import-artifacts-bucket
bb blambda terraform apply
```
