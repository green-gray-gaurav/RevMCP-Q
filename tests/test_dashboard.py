from starlette.testclient import TestClient

from rmcpq.app import create_app


def test_dashboard_assets_local_security_and_existing_api(tmp_path):
    with TestClient(
        create_app(tmp_path / "dashboard.sqlite"), base_url="http://127.0.0.1:8791"
    ) as client:
        for path in ("/", "/dashboard"):
            page = client.get(path)
            assert page.status_code == 200
            assert "text/html" in page.headers["content-type"]
            assert "Queue dashboard" in page.text
            assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
            assert page.headers["cache-control"] == "no-store"
            assert client.get(path, headers={"Host": "evil.example"}).status_code == 403
        script = client.get("/assets/dashboard.js")
        assert script.status_code == 200
        assert "textContent" in script.text and "innerHTML" not in script.text
        assert client.get("/assets/dashboard.css").status_code == 200
        assert client.get("/assets/favicon.svg").status_code == 200
        assert (
            client.get(
                "/assets/dashboard.js", headers={"Origin": "https://evil.example"}
            ).status_code
            == 403
        )
        assert client.get("/v1/health").json()["ok"]
        job = client.post(
            "/v1/jobs", json={"queue": "a", "instruction": "<script>alert(1)</script>"}
        ).json()
        assert client.get("/v1/jobs/" + job["id"]).json()["instruction"] == job["instruction"]
        assert "alert(1)" not in client.get("/").text
