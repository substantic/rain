let SERVER_URL = "/";

if (window.location.href.startsWith("http://localhost:3000")) {
    console.log("DEV mode detected, connecting to http://localhost:8080/");
    SERVER_URL = "http://localhost:8080/";
}

export function fetch_from_server(link, body, method="POST") {
    return fetch(SERVER_URL + link, {
        method: method,
        mode: 'cors',
        cache: 'no-cache',
        body: body
    })
}

export function fetch_json_from_server(link, body, method) {
  return fetch_from_server(link, JSON.stringify(body), method).then(response => {
        return response.json();
  })
}

export function fetch_events(search_criteria, callback, on_error, update) {
    let last_event_id = null;

    let _fetch_events = () => {
        if (last_event_id) {
            search_criteria.id = {value: last_event_id, mode: ">"}
        }
        fetch_json_from_server("events", search_criteria).then(response => {
            for(let event of response) {
                callback(event)
            }
            if (response.length > 0) {
                let id = response[response.length - 1].id;
                if (id > last_event_id) {
                    last_event_id = id;
                }
            }
            if (response.length !== 0 && update) {
                update();
            }
        }).catch(error => {
            console.log(error);
            on_error("Failed to fetch data from the server");
            clearInterval(timer);
            timer = null;
        });
    }

    _fetch_events();
    let timer = setInterval(_fetch_events, 1000);

    return (() => {
        if (timer) {
            clearInterval(timer);
            timer = null;
        }
    });
}