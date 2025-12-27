from isbn_harvester.core.tasks import build_tasks


def test_build_tasks_default_non_empty() -> None:
    tasks = build_tasks(fiction_only=False)
    assert len(tasks) > 0
