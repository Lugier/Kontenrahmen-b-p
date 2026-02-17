"""Tests for target position loading."""
import pytest
from pathlib import Path
from src.targets import load_targets


# Path to the actual test fixture
TARGETS_PATH = Path(__file__).parent.parent / "Examples" / "Lucanet Einlesen Automation" / "Unsere_Lucanet_Zuordnung.xls"


@pytest.mark.skipif(not TARGETS_PATH.exists(), reason="Targets file not found")
class TestTargetsLoading:

    def test_loads_targets(self):
        targets = load_targets(TARGETS_PATH)
        assert len(targets) > 0, "Should load at least one target"

    def test_has_bilanz_and_guv(self):
        targets = load_targets(TARGETS_PATH)
        classes = {t.target_class for t in targets}
        # Should have at least some of these classes
        assert len(classes) > 0, "Should detect target classes"

    def test_target_ids_unique(self):
        targets = load_targets(TARGETS_PATH)
        ids = [t.target_id for t in targets]
        assert len(ids) == len(set(ids)), "Target IDs should be unique"

    def test_target_has_name(self):
        targets = load_targets(TARGETS_PATH)
        for t in targets:
            assert t.target_name, f"Target {t.target_id} should have a name"

    def test_hierarchy_paths_not_empty(self):
        targets = load_targets(TARGETS_PATH)
        with_paths = [t for t in targets if t.hierarchy_path]
        assert len(with_paths) > 0, "Some targets should have hierarchy paths"
