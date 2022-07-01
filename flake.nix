{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, utils }:
    let
      out = system:
        let pkgs = nixpkgs.legacyPackages."${system}";
        in {

          devShell = pkgs.mkShell {
            buildInputs = with pkgs; [ python3Packages.poetry ];
          };

          packages = {
            default = self.packages.${system}.khinsider-scraper;
            khinsider-scraper = with pkgs.poetry2nix;
              mkPoetryApplication {
                projectDir = ./.;
                preferWheels = true;
              };

            khinsider-scraper-docker = pkgs.dockerTools.buildImage {
              name = "ghcr.io/astridyu/khinsider-scraper";
              contents = self.packages."${system}".khinsider-scraper;
              config = { Cmd = [ "${pkgs.bashInteractive}/bin/bash" ]; };
            };
          };

          apps = {
            khinsider-scraper = utils.lib.mkApp {
              drv = self.packages."${system}".khinsider-scraper;
            };
            default = self.apps.${system}.khinsider-scraper;
          };
        };
    in with utils.lib; eachSystem defaultSystems out;

}
