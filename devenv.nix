{ pkgs, multiverse, ... }:

{
  dotenv.enable = true;

  packages = with pkgs; [
    lld

    just
    # just-lsp
    multiverse.just-lsp."0.10.0"

    cargo-audit
    cargo-deny
    cargo-release
    cargo-watch

    wrangler

    # Markdown link check
    lychee
  ];

  dioxus = {
    enable = true;
  };

  worker-build = {
    enable = true;

    # TODO: drop once nixpkgs has worker-build 0.9.0.
    package = pkgs.worker-build.overrideAttrs (
      finalAttrs: prev: {
        version = "0.9.0";

        src = pkgs.fetchFromGitHub {
          owner = "cloudflare";
          repo = "workers-rs";
          tag = "v${finalAttrs.version}";
          hash = "sha256-zmeQp7db2rwfECp9p5NthKvzsZFnQPFO+eueIhFsIwE=";
          fetchSubmodules = true;
        };

        cargoHash = "sha256-EbRwSrxRys2EcDfB88apmH5Rm0NUWLKtmGPS77fAhpI=";

        # zstd-sys links the system zstd through pkg-config.
        buildInputs = prev.buildInputs ++ [ pkgs.zstd ];
      }
    );
  };

  languages = {
    rust = {
      enable = true;
      channel = "stable";
      targets = [ "wasm32-unknown-unknown" ];
    };

    javascript = {
      enable = true;
      npm.enable = true;
    };

    tailwindcss = {
      enable = true;
    };
  };
}
