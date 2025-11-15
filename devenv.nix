{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/basics/
  env = {
    PYTHONPATH = "./src";
    UV_CACHE_DIR = "${config.env.DEVENV_STATE}/uv-cache";
    UV_COMPILE_BYTECODE = "1";
    UV_LINK_MODE = "copy";
    # Override devenv's restrictive setting to allow uv-managed Python
    UV_PYTHON_PREFERENCE = lib.mkForce "managed";
  };
  
  # https://devenv.sh/packages/
  packages = with pkgs; [
    git
    just  # command runner
  ];

  # Python environment with uv
  languages.python = {
    enable = true;
    uv = {
      enable = true;
      sync.enable = false;
    };
  };

  # https://devenv.sh/tasks/
  tasks = {
    "uv:setup" = {
      exec = ''
        if [ ! -f pyproject.toml ]; then
          echo "Initializing Python project with uv..."
          uv init --no-readme
          uv add --dev pytest ruff ty
        else
          echo "Found existing pyproject.toml, syncing dependencies..."
        fi
        uv sync
      '';
      before = [ "devenv:enterShell" ];
    };
  };

  # https://devenv.sh/scripts/
  scripts.test.exec = "uv run pytest";
  scripts.lint.exec = "uv run ruff check .";
  scripts.format.exec = "uv run ruff format .";
  scripts.check.exec = "uv run ty check .";

  # https://devenv.sh/reference/options/#git-hooks
  git-hooks.hooks = {
    ruff.enable = true;
    ruff-format.enable = true;
    # No ty hook yet, run manually
  };

  # Enter shell message
  enterShell = ''
    echo "🐍 Python development environment activated"
    echo "Available commands:"
    echo "  - uv run <command>   # Run with project dependencies"
    echo "  - test               # Run tests"
    echo "  - lint               # Check code"
    echo "  - format             # Format code"
    echo "  - check              # Type checking"
  '';
}
