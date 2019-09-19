
let hash_sha256: string => Digestif.SHA256.t;
let hmac_sha256: (~key: string, string) => Digestif.SHA256.t;
let to_hex: Digestif.SHA256.t => string;

let make_signing_key:
  (
    ~bypass_cache: bool=?,
    ~date: string,
    ~region: string,
    ~credentials: Credentials.t,
    ~service: string,
    unit
  ) =>
  Digestif.SHA256.t;

let make_scope: (~date: string, ~region: string, ~service: string) => string;

let string_to_sign:
  (
    ~date: string,
    ~time: string,
    ~verb: string,
    ~path: string,
    ~query: list((string, string)),
    ~headers: Headers.t(string),
    ~payload_sha: string,
    ~scope: string
  ) =>
  (string, string);

let make_signature:
  (
    ~date: string,
    ~time: string,
    ~verb: string,
    ~path: string,
    ~headers: Headers.t(string),
    ~query: list((string, string)),
    ~scope: string,
    ~signing_key: Digestif.SHA256.t,
    ~payload_sha: string
  ) =>
  (string, string);

let make_auth_header:
  (
    ~credentials: Credentials.t,
    ~scope: string,
    ~signed_headers: string,
    ~signature: string
  ) =>
  string;

let chunk_signature:
  (
    ~signing_key: Digestif.SHA256.t,
    ~date: string,
    ~time: string,
    ~scope: string,
    ~previous_signature: string,
    ~sha: Digestif.SHA256.t
  ) =>
  Digestif.SHA256.t;


/** This makes a presigned url that can be used to upload or download a file from s3 without any credentials other than those embedded in the url. [verb] should be either the string GET for download or PUT for upload.*/

let make_presigned_url:
  (
    ~scheme: [ | `Http | `Https]=?,
    ~host: string=?,
    ~port: int=?,
    ~credentials: Credentials.t,
    ~date: Ptime.t,
    ~region: Region.t,
    ~path: string,
    ~bucket: string,
    ~verb: [ | `Get | `Put],
    ~duration: int,
    unit
  ) =>
  Uri.t;
