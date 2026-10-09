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
