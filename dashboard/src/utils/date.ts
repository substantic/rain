const isFirefox = navigator.userAgent.toLowerCase().indexOf("firefox") > -1;

export function parseDate(date: string): Date {
  if (isFirefox) {
    // Firefox cannot parse " UTC"
    date = date.slice(0, -4) + "Z";
  }
  return new Date(date);
}
