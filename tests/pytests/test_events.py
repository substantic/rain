from time import sleep

import requests


def test_event_filter(test_env):
    test_env.start(1)
    with test_env.client.new_session() as session:
        session_id = session.session_id

    sleep(1)

    data = {
        "event_types": [
            {"value": "SessionClosed", "mode": "="},
            {"value": "SessionNew", "mode": "="}]
    }

    res = requests.post('http://localhost:{}/events'.format(test_env.default_http_port), json=data)
    events = res.json()

    assert len(events) == 2
    assert events[0]['event']['session'] == session_id
    assert events[0]['event']['session'] == events[1]['event']['session']

    by_id = sorted(events, key=lambda ev: ev['id'])
    assert by_id[0]['event']['type'] == 'SessionNew'
    assert by_id[1]['event']['type'] == 'SessionClosed'
