const isFirefox = navigator.userAgent.toLowerCase().indexOf("firefox") > -1;

export function parseDate(date: string) {
  if (isFirefox) {
    // Firefox cannot parse " UTC"
    date = date.slice(0, -4) + "Z";
    console.log("DATE", date);
  }
  return new Date(date);
}
