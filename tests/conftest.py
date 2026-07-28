import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
WORKER_ROOT = ROOT / "worker"
sys.path.insert(0, str(WORKER_ROOT))
