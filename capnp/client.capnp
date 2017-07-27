@0xb3195a92eff52478;

struct Info {
  nWorkers @0 :Int32;
}

interface ClientService {
  getInfo @0 () -> Info;
}
