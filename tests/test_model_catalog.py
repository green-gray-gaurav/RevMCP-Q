from types import SimpleNamespace

import pytest
from starlette.testclient import TestClient

from rmcpq.app import create_app
from rmcpq.controls import ModelCatalog, codex_models
from rmcpq.models import BrokerError
from rmcpq.worker import WorkerError


@pytest.mark.anyio
async def test_catalog_caches_success_and_supports_refresh(tmp_path, monkeypatch):
    import rmcpq.controls as module

    calls = []

    async def load(cwd):
        calls.append(cwd)
        return {"models": [{"id": "model-one", "label": "Model One"}], "default_model": "model-one"}

    monkeypatch.setattr(module, "codex_models", load)
    catalog = ModelCatalog(tmp_path)
    assert (await catalog.get())["default_model"] == "model-one"
    await catalog.get()
    assert len(calls) == 1
    await catalog.get(force=True)
    assert len(calls) == 2


@pytest.mark.anyio
async def test_catalog_uses_signed_in_host_and_collects_visible_pages(tmp_path, monkeypatch):
    import rmcpq.hosts.codex as module

    def model(slug, hidden=False):
        return SimpleNamespace(model=slug, id=slug, display_name=slug.upper(), hidden=hidden)

    class Host:
        def __init__(self, options):
            self.model = "model-one"
            self.codex = self
            self._client = self
            self.checked = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def preflight(self):
            self.checked = True

        async def models(self):
            assert self.checked
            return SimpleNamespace(
                data=[model("model-one"), model("hidden", True)], next_cursor="page-two"
            )

        async def request(self, name, params, **kwargs):
            assert name == "model/list" and params["cursor"] == "page-two"
            return SimpleNamespace(data=[model("model-two")], next_cursor=None)

    monkeypatch.setattr(module, "CodexHost", Host)
    catalog = await codex_models(tmp_path)
    assert [model["id"] for model in catalog["models"]] == ["model-one", "model-two"]
    assert catalog["default_model"] == "model-one"


@pytest.mark.anyio
async def test_auth_failure_produces_safe_catalog_error(tmp_path, monkeypatch):
    import rmcpq.hosts.codex as module

    class Host:
        def __init__(self, options):
            pass

        async def __aenter__(self):
            raise WorkerError("subscription_required", "Sign in with ChatGPT", fatal=True)

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr(module, "CodexHost", Host)
    with pytest.raises(BrokerError) as exc:
        await codex_models(tmp_path)
    assert exc.value.code == "subscription_required"


def test_catalog_endpoint_and_dropdown_markup(tmp_path, monkeypatch):
    app = create_app(tmp_path / "catalog.sqlite")
    refreshes = []

    async def get(force=False):
        refreshes.append(force)
        return {"models": [{"id": "model-one", "label": "Model One"}], "default_model": "model-one"}

    monkeypatch.setattr(app.state.model_catalog, "get", get)
    with TestClient(app, base_url="http://127.0.0.1:8791") as client:
        assert client.get("/v1/models").json()["models"][0]["id"] == "model-one"
        assert client.get("/v1/models?refresh=1").headers["cache-control"] == "no-store"
        assert refreshes == [False, True]
        assert client.get("/v1/models", headers={"Host": "evil.example"}).status_code == 403
        page = client.get("/").text
        assert 'select name="model"' in page
        assert 'input name="model"' not in page
