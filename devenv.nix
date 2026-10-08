{ pkgs, multiverse, ... }:

{
  dotenv.enable = true;

  packages = with pkgs; [
    lld

    just
    # just-lsp
    multiverse.just-lsp."0.10.0"

    cargo-audit
    cargo-binstall
    cargo-deny
    cargo-dist
    cargo-release
    cargo-watch

    multiverse.dioxus-cli."0.7.10"
    # wasm-opt for dx: the Nix build of dx does not download it
    binaryen
    multiverse.worker-build."0.8.7"
    # wasm-bindgen-cli_0_2_129
    multiverse.wasm-bindgen-cli."0.2.129"
    wrangler

    # Markdown link check
    lychee
  ];

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
