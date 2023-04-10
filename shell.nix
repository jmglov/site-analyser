{ pkgs ? import <nixpkgs> { } }:

with pkgs;
let
  arch = if stdenv.isAarch64 then "aarch64" else "amd64";
  osName = if stdenv.isDarwin then
    "macos"
  else if stdenv.isLinux then
    "linux"
  else
    null;
  sha256 = assert !isNull osName;
    {
      linux = {
        aarch64 =
          "f187204e38f2ae430fb0f167807022d43c32e15e8f91c225819e8a72965092f6";
        amd64 =
          "b1772d2b04399ed981803b380307888a58068803ecf2c0cd22c43e83b811a9f8";
      };
    }.${osName}.${arch};

  babashka = stdenv.mkDerivation rec {
    pname = "babashka";
    version = "1.3.176";

    src = builtins.fetchurl {
      inherit sha256;
      url =
        "https://github.com/babashka/babashka/releases/download/v${version}/babashka-${version}-${osName}-${arch}-static.tar.gz";
    };

    dontFixup = true;
    dontUnpack = true;

    installPhase = ''
      mkdir -p $out/bin
      cd $out/bin && tar xvzf $src
    '';
  };

in mkShell { buildInputs = [ babashka terraform ]; }
