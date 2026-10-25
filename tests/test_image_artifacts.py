import json

import pytest

from rmcpq.hosts.artifacts import image_directory, save_reported_images
from rmcpq.hosts.codex import CodexHost
from rmcpq.worker import WorkerError, WorkerOptions


@pytest.fixture
def image_paths(tmp_path):
    source_dir = tmp_path / "codex" / "generated_images" / "thread-one"
    source_dir.mkdir(parents=True)
    source = source_dir / "exec-output.png"
    source.write_bytes(b"image fixture")
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    return source_dir, source, cwd


def save(image_paths, *, context=None, sandbox="workspace-write", result=None, source=None):
    directory, default_source, cwd = image_paths
    source = source or default_source
    return save_reported_images(
        result or {"filepath": str(source)},
        [str(source)],
        source_dir=directory,
        cwd=cwd,
        sandbox=sandbox,
        context=context,
        job_id="12345678-abcd",
    )


def test_generated_image_copied_and_result_path_rewritten(image_paths):
    directory, source, cwd = image_paths
    result, artifacts = save(image_paths, context={"filepath": str(cwd)})
    output = cwd / "image-12345678.png"
    assert output.read_bytes() == source.read_bytes()
    assert source.is_file()
    assert result == {"filepath": str(output)} and artifacts == [str(output)]
    again, paths = save(image_paths)
    assert paths == [str(cwd / "image-12345678-v2.png")]
    assert output.read_bytes() == source.read_bytes()


def test_explicit_filename_and_nested_json_rewritten(image_paths):
    _, source, cwd = image_paths
    result, paths = save(
        image_paths, context={"filepath": "renders/sky.png"}, result={"files": [source.as_posix()]}
    )
    assert paths == [str(cwd / "renders" / "sky.png")]
    assert result == {"files": paths}


@pytest.mark.parametrize("kind", ["outside", "read-only", "format", "other-thread"])
def test_artifact_copy_does_not_escape_permissions_or_thread(image_paths, kind):
    directory, source, cwd = image_paths
    kwargs = {}
    if kind == "outside":
        kwargs["context"] = {"filepath": str(cwd.parent / "outside")}
    elif kind == "read-only":
        kwargs["sandbox"] = "read-only"
    elif kind == "format":
        kwargs["context"] = {"filepath": str(cwd / "sky.jpg")}
    else:
        other = directory.parent / "thread-other" / "image.png"
        other.parent.mkdir()
        other.write_bytes(b"other job")
        kwargs["source"] = other
    with pytest.raises(WorkerError):
        save(image_paths, **kwargs)
    assert list(cwd.iterdir()) == []


def test_missing_generated_source_is_not_published(image_paths):
    _, source, _ = image_paths
    source.unlink()
    with pytest.raises(WorkerError) as exc:
        save(image_paths)
    assert exc.value.code == "missing_artifact"


@pytest.mark.anyio
async def test_host_finishes_generated_image_outcome_with_workspace_path(image_paths, monkeypatch):
    directory, source, cwd = image_paths
    import rmcpq.hosts.codex as module

    monkeypatch.setattr(module, "image_directory", lambda thread: directory)
    host = CodexHost(WorkerOptions(cwd=cwd))
    final = await host.finish(
        json.dumps(
            {
                "outcome": "success",
                "result": {"filepath": str(source)},
                "error": None,
                "artifacts": [str(source)],
            }
        ),
        "json",
        job={"id": "12345678-abcd", "context": {"filepath": str(cwd)}},
        thread_id="thread-one",
    )
    assert json.loads(final) == {"filepath": str(cwd / "image-12345678.png")}


def test_image_directory_is_thread_scoped(tmp_path, monkeypatch):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    assert image_directory("thread-one") == tmp_path / "generated_images" / "thread-one"
    with pytest.raises(WorkerError):
        image_directory("../other")
