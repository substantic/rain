import { EventWrapper } from "../lib/event";

let SERVER_URL = "/";

if (window.location.href.startsWith("http://localhost:3000")) {
  console.log("DEV mode detected, connecting to http://localhost:8080/");
  SERVER_URL = "http://localhost:8080/";
}

export function fetchFromServer(
  link: string,
  body: string,
  method: string = "POST"
) {
  return fetch(SERVER_URL + link, {
    method,
    mode: "cors",
    cache: "no-cache",
    body
  });
}

export function fetchJsonFromServer(
  link: string,
  body: {},
  method: string = "POST"
) {
  return fetchFromServer(link, JSON.stringify(body), method).then(response => {
    return response.json();
  });
}

export function fetchEvents(
  searchCriteria: any,
  callback: (events: EventWrapper[]) => void,
  onError: (error: string) => void
) {
  let lastEventId: number = null;

  const fetch = () => {
    if (lastEventId) {
      searchCriteria.id = { value: lastEventId, mode: ">" };
    }
    fetchJsonFromServer("events", searchCriteria)
      .then(response => {
        if (response.length > 0) {
          callback(response);
          const id = response[response.length - 1].id;
          if (id > lastEventId) {
            lastEventId = id;
          }
        }
      })
      .catch(error => {
        console.log(error);
        onError("Failed to fetch data from the server");
        clearInterval(timer);
        timer = null;
      });
  };

  fetch();
  let timer = setInterval(fetch, 1000);

  return () => {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  };
}
