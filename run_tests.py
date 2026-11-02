"""Run every problem's tests in isolation:  python run_tests.py"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
failed = []
dirs = sorted(p for p in (ROOT / "problems").iterdir() if (p / "test_solution.py").exists())
for d in dirs:
    r = subprocess.run([sys.executable, "-m", "unittest", "-q", "test_solution"], cwd=d,
                       capture_output=True, text=True)
    ok = r.returncode == 0
    print(f"{'PASS' if ok else 'FAIL'}  {d.name}")
    if not ok:
        failed.append(d.name)
        print(r.stdout + r.stderr)
print(f"\n{len(dirs) - len(failed)}/{len(dirs)} problems passing")
sys.exit(1 if failed else 0)
