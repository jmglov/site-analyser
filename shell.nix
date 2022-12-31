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
          "7f90e48832d498b77c260dd5af03c4206b2bb175726d21616737537e75b0d141";
        amd64 =
          "c72d1682d2a77421939f63987ceeca09bbd8086f96de5e1ed1a46a9a70eada88";
      };
      macos = {
        aarch64 =
          "11c4b4bd0b534db1ecd732b03bc376f8b21bbda0d88cacb4bbe15b8469029123";
        amd64 =
          "379a4f50ba302d3c5a5e2687f9e1dfe02acbd9a87e0b4619f4f0791326f340d0";
      };
    }.${osName}.${arch};

  babashka = stdenv.mkDerivation rec {
    pname = "babashka";
    version = "1.0.168";

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
