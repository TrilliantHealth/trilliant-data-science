import subprocess
import sys


def test_ml_free_modules_import_without_ml_deps() -> None:
    """`io` and `search_space` must load with no ML frameworks installed."""
    code = (
        "import sys; "
        "sys.modules['sklearn'] = None; sys.modules['skopt'] = None; sys.modules['xgboost'] = None; "
        "import thds.mllegos.io; import thds.mllegos.search_space; print('ok')"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
