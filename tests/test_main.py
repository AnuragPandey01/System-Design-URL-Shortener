def test_read_root(client):
    response = client.get("/status")
    assert response.status_code == 200
    assert response.json() == {"message": "ok"}
