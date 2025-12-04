{ pkgs, lib, config, inputs, ... }:

{
  env.RUSTC_WRAPPER = "${pkgs.sccache}/bin/sccache";

  # https://devenv.sh/packages/
  packages = [ pkgs.git pkgs.libyaml pkgs.openssl pkgs.zlib pkgs.sccache pkgs.wasm-pack pkgs.coz];

  # https://devenv.sh/languages/
  languages.rust = {
    enable = true;
    channel = "stable";
    mold.enable = true;
    targets = [ "x86_64-unknown-linux-musl" ];
  };

  languages.ruby.enable = true;

  # https://devenv.sh/pre-commit-hooks/
  git-hooks.hooks = {
    rustfmt.enable = true;
  };

}
