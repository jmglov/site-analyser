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

Refer to the [Building](https://github.com/jmglov/blambda#building) and
[Dependencies](https://github.com/jmglov/blambda#dependencies) sections of the
Blambda documentation.

## Deploying

Refer to the [Deploying](https://github.com/jmglov/blambda#deploying) section of
the Blambda documentation.
