"""Move reported built-in image outputs into the authorized job workspace."""

import os
import re
import shutil
from pathlib import Path

from ..worker import WorkerError


def image_directory(thread_id):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", thread_id):
        raise WorkerError("invalid_thread", "Invalid image output thread identifier")
    codex_home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex").expanduser()
    return codex_home / "generated_images" / thread_id


def destination_for(context, cwd):
    value = context.get("filepath") if isinstance(context, dict) else None
    if value is None:
        return cwd, False
    if not isinstance(value, str) or not value.strip():
        raise WorkerError("artifact_destination", "Context filepath must be a nonempty path string")
    target = Path(value)
    if not target.is_absolute():
        target = cwd / target
    target = target.resolve()
    if not target.is_relative_to(cwd):
        raise WorkerError(
            "artifact_destination", "Image destination must stay within the worker cwd"
        )
    return target, not target.is_dir() and bool(target.suffix)


def replace_paths(value, replacements):
    if isinstance(value, str):
        for source, target in replacements.items():
            value = value.replace(source, target)
        return value
    if isinstance(value, list):
        return [replace_paths(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: replace_paths(item, replacements) for key, item in value.items()}
    return value


def save_reported_images(result, artifacts, *, source_dir, cwd, sandbox, context, job_id):
    """Copy only declared files from this thread, never guess another job's output.

    Plain workspace artifacts pass through. Writing here follows the same cwd and
    read-only restrictions as the Codex thread rather than escaping its sandbox.
    """
    cwd = Path(cwd).resolve()
    source_dir = Path(source_dir).resolve()
    replacements, paths = {}, []
    for value in artifacts:
        source = Path(value)
        resolved = source.resolve()
        if not resolved.is_relative_to(source_dir):
            if resolved.is_relative_to(source_dir.parent):
                raise WorkerError(
                    "artifact_source", "Generated image belongs to a different thread"
                )
            # A path spelled inside the thread folder must not escape via symlinks.
            if source.is_relative_to(source_dir):
                raise WorkerError(
                    "artifact_source", "Generated image path escapes its thread folder"
                )
            paths.append(value)
            continue
        if sandbox != "workspace-write":
            raise WorkerError(
                "artifact_permission", "Saving generated images requires a workspace-write worker"
            )
        if not resolved.is_file() or resolved.stat().st_size == 0:
            raise WorkerError("missing_artifact", "Generated image output file is missing or empty")
        target, is_file = destination_for(context, cwd)
        if is_file:
            if len(artifacts) != 1:
                raise WorkerError(
                    "artifact_destination", "Use a destination directory for multiple artifacts"
                )
            if target.suffix.lower() != resolved.suffix.lower():
                raise WorkerError(
                    "artifact_destination",
                    "Output filename extension must match the generated image format",
                )
        else:
            target = target / f"image-{job_id[:8]}{resolved.suffix}"
        parent = target.parent.resolve()
        if not parent.is_relative_to(cwd):
            raise WorkerError(
                "artifact_destination", "Image destination must stay within the worker cwd"
            )
        parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation preserves existing files, including during retries.
        for version in range(1, 1001):
            candidate = (
                target
                if version == 1
                else target.with_name(f"{target.stem}-v{version}{target.suffix}")
            )
            try:
                output = candidate.open("xb")
            except FileExistsError:
                continue
            try:
                with output, resolved.open("rb") as input_file:
                    shutil.copyfileobj(input_file, output)
            except BaseException:
                candidate.unlink(missing_ok=True)
                raise
            break
        else:
            raise WorkerError("artifact_destination", "Could not allocate a new output filename")
        saved = str(candidate)
        replacements[value] = saved
        replacements[resolved.as_posix()] = saved
        replacements[str(resolved)] = saved
        paths.append(saved)
    return replace_paths(result, replacements), paths
