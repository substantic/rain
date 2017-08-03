
def test_get_info(test_env):
    test_env.start(0)  # Start server with no workers
    client = test_env.client

    info = client.get_server_info()
    assert info["n_workers"] == 0
