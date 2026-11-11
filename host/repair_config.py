"""Add the required config version to an existing pre-0.1.3 installation."""
import argparse
import os
import tempfile
from pathlib import Path

import tomlkit


def repair(path: Path) -> None:
    if not path.exists():
        return
    config = tomlkit.parse(path.read_text(encoding="utf-8"))
    if "plugin" not in config:
        config["plugin"] = tomlkit.table()
    plugin = config["plugin"]
    if not isinstance(plugin, dict):
        raise ValueError("config.toml: plugin must be a TOML table")
    if "config_version" in plugin:
        return
    plugin["config_version"] = "1.0.0"
    with tempfile.NamedTemporaryFile(dir=path.parent, mode="w", encoding="utf-8", delete=False) as file:
        temporary = Path(file.name)
        file.write(tomlkit.dumps(config))
    try:
        temporary.chmod(path.stat().st_mode & 0o777)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    repair(parser.parse_args().config)
