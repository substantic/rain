
const is_firefox = navigator.userAgent.toLowerCase().indexOf('firefox') > -1;

export function parse_date(string) {
    if (is_firefox) {
        // Firefox cannot parse " UTC"
        string = string.slice(0, -4) + "Z";
    }
    return new Date(string);
}