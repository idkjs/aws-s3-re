type vendor;

type t =
  | Ap_northeast_1
  | Ap_northeast_2
  | Ap_northeast_3
  | Ap_southeast_1
  | Ap_southeast_2
  | Ap_south_1
  | Eu_central_1
  | Cn_northwest_1
  | Cn_north_1
  | Eu_west_1
  | Eu_west_2
  | Eu_west_3
  | Sa_east_1
  | Us_east_1
  | Us_east_2
  | Us_west_1
  | Us_west_2
  | Ca_central_1
  | Other(string)
  | Vendor(vendor);

let vendor: (~region_name: string, ~host: string, ~port: int) => t;

let minio: (~host: string, ~port: int) => t;

type endpoint = {
  inet: [ | `V4 | `V6],
  scheme: [ | `Http | `Https],
  host: string,
  port: int,
  region: t,
};

let endpoint:
  (~inet: [ | `V4 | `V6], ~scheme: [ | `Http | `Https], t) => endpoint;

let to_string: t => string;
let of_string: string => t;
let of_host: string => t;
