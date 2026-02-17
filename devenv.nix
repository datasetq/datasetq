{ pkgs, lib, config, inputs, ... }:

{
  env.RUSTC_WRAPPER = "${pkgs.sccache}/bin/sccache";

  # https://devenv.sh/packages/
  packages = [ pkgs.git pkgs.libyaml pkgs.openssl pkgs.zlib pkgs.sccache pkgs.wasm-pack pkgs.coz pkgs.dpkg pkgs.clang];

  # https://devenv.sh/languages/
  languages.rust = {
    enable = true;
    channel = "stable";
    mold.enable = false;
    targets = [ "x86_64-unknown-linux-musl" "wasm32-unknown-unknown"];
    components = [  "rustc" "cargo" "clippy" "rustfmt" "rust-analyzer" "llvm-tools-preview"];
  };

  languages.ruby.enable = true;
  languages.zig.enable = true;

  # https://devenv.sh/pre-commit-hooks/
  git-hooks.hooks = {
    rustfmt.enable = true;
  };

}
