"""Install the version-specific RPC capability into the pinned MaiBot image."""
from pathlib import Path
import argparse
import shutil

parser = argparse.ArgumentParser()
parser.add_argument("--root", type=Path, default=Path("/MaiMBot"))
args = parser.parse_args()
source = Path(__file__).resolve().parent / "passive_memory.py"
target = args.root / "src/plugin_runtime/capabilities"
registry = target / "registry.py"
content = registry.read_text(encoding="utf-8")
anchor = '    cap_service.register_capability("webui.claim_upload", claim_upload)\n'
registration = '    cap_service.register_capability("passive_memory.batch", passive_memory.handle)\n'
if content.count(anchor) != 1:
    raise SystemExit("Unsupported MaiBot capability registry")
if registration not in content:
    original_import = "from . import desktop_pet,"
    if content.count(original_import) != 1:
        raise SystemExit("Unsupported MaiBot capability imports")
    content = content.replace(original_import, "from . import passive_memory, desktop_pet,", 1)
    content = content.replace(anchor, anchor + registration, 1)
    shutil.copyfile(source, target / source.name)
    registry.write_text(content, encoding="utf-8")
else:
    shutil.copyfile(source, target / source.name)
