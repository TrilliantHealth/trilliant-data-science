from pathlib import Path

from thds import core
from thds.mops import config


def test_nested_tables_in_a_config_file_are_loaded(tmp_path: Path):
    config_file = tmp_path / ".mops.toml"
    config_file.write_text(
        '[mops.memo."some.module--nested_table_func"]\nmemospace = "adls://sa/container/nested"\n'
    )

    config._load_global_defaults(config_file)

    assert (
        core.config.config_by_name("mops.memo.some.module--nested_table_func.memospace")()
        == "adls://sa/container/nested"
    )


def test_dotted_top_level_keys_in_a_config_file_are_loaded(tmp_path: Path):
    config_file = tmp_path / ".mops.toml"
    config_file.write_text(
        '"mops.memo.some.module--dotted_key_func.memospace" = "adls://sa/container/dotted"\n'
    )

    config._load_global_defaults(config_file)

    assert (
        core.config.config_by_name("mops.memo.some.module--dotted_key_func.memospace")()
        == "adls://sa/container/dotted"
    )


def test_config_for_other_packages_is_not_loaded(tmp_path: Path):
    config_file = tmp_path / ".mops.toml"
    config_file.write_text(
        '[thds.someapp]\nsetting = "x"\n\n[thds.mops_queue]\nroot = "adls://sa/container"\n'
    )

    config._load_global_defaults(config_file)

    loaded = core.config.get_all_config()
    assert "thds.someapp.setting" not in loaded
    assert "thds.mops_queue.root" not in loaded
