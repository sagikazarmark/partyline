{ config, lib, pkgs, multiverse, ... }:

let
  cfg = config.languages.dioxus;

  # The Cargo.lock to read versions from: the one set, or the one at the root if it exists.
  cargoLock =
    if cfg.cargoLock != null then
      cfg.cargoLock
    else if builtins.pathExists (config.devenv.root + "/Cargo.lock") then
      config.devenv.root + "/Cargo.lock"
    else
      null;

  # The version of a crate in Cargo.lock, or null without a Cargo.lock or the crate.
  lockedVersion = crate:
    if cargoLock == null then
      null
    else
      let
        lock = builtins.fromTOML (builtins.readFile cargoLock);
        package = lib.findFirst (p: p.name == crate) null (lock.package or [ ]);
      in
      if package == null then null else package.version;

  # A package at an exact version from nixpkgs-multiverse.
  fromMultiverse = attr: version:
    multiverse.${attr}.${version}
      or (throw "nixpkgs-multiverse has no ${attr} ${version}");

  # wasm-bindgen-cli at an exact version: nixpkgs first, then nixpkgs-multiverse.
  wasmBindgenCli = version:
    let
      versioned = "wasm-bindgen-cli_${lib.replaceStrings [ "." ] [ "_" ] version}";
    in
    if pkgs.wasm-bindgen-cli.version == version then
      pkgs.wasm-bindgen-cli
    else if pkgs ? ${versioned} then
      pkgs.${versioned}
    else
      fromMultiverse "wasm-bindgen-cli" version;

  versionDescription = crate: ''
    The version to install. It must match the `${crate}` crate exactly.
    Defaults to the version of `${crate}` in Cargo.lock, if there is one.
  '';
in
{
  options.languages.dioxus = {
    enable = lib.mkEnableOption "the Dioxus CLI (dx)";

    cargoLock = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      description = ''
        The Cargo.lock to read the dioxus and wasm-bindgen versions from.
        When null, Cargo.lock at the project root is used if it exists.
      '';
    };

    version = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = lockedVersion "dioxus";
      defaultText = lib.literalMD "the version of `dioxus` in Cargo.lock, or null";
      description = ''
        ${versionDescription "dioxus"}
        A version is installed from nixpkgs-multiverse. When null, `pkgs.dioxus-cli` is installed.
      '';
    };

    package = lib.mkOption {
      type = lib.types.package;
      default = if cfg.version == null then pkgs.dioxus-cli else fromMultiverse "dioxus-cli" cfg.version;
      defaultText = lib.literalMD "`dioxus-cli` at `version`, or `pkgs.dioxus-cli`";
      description = "The dioxus-cli package. Setting it overrides `version`.";
    };

    wasm-bindgen = {
      enable = lib.mkEnableOption "wasm-bindgen-cli, which dx and wasm-bindgen-test-runner need at the crate's exact version" // {
        default = true;
      };

      version = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = lockedVersion "wasm-bindgen";
        defaultText = lib.literalMD "the version of `wasm-bindgen` in Cargo.lock, or null";
        description = ''
          ${versionDescription "wasm-bindgen"}
          A version is installed from nixpkgs if it has it, otherwise from nixpkgs-multiverse.
          When null, `pkgs.wasm-bindgen-cli` is installed.
        '';
      };

      package = lib.mkOption {
        type = lib.types.package;
        default = if cfg.wasm-bindgen.version == null then pkgs.wasm-bindgen-cli else wasmBindgenCli cfg.wasm-bindgen.version;
        defaultText = lib.literalMD "`wasm-bindgen-cli` at `version`, or `pkgs.wasm-bindgen-cli`";
        description = "The wasm-bindgen-cli package. Setting it overrides `version`.";
      };
    };

    binaryen = {
      enable = lib.mkEnableOption "binaryen, for wasm-opt. The Nix build of dx does not download wasm-opt itself" // {
        default = true;
      };

      version = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "The binaryen version to install from nixpkgs-multiverse. When null, `pkgs.binaryen` is installed.";
      };

      package = lib.mkOption {
        type = lib.types.package;
        default = if cfg.binaryen.version == null then pkgs.binaryen else fromMultiverse "binaryen" cfg.binaryen.version;
        defaultText = lib.literalMD "`binaryen` at `version`, or `pkgs.binaryen`";
        description = "The binaryen package. Setting it overrides `version`.";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    packages =
      [ cfg.package ]
      ++ lib.optional cfg.wasm-bindgen.enable cfg.wasm-bindgen.package
      ++ lib.optional cfg.binaryen.enable cfg.binaryen.package;
  };
}
