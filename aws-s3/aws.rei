
module Make:
  (Io: Types.Io) =>
   {
    open Io;
    let make_request:
      (
        ~endpoint: Region.endpoint,
        ~expect: bool=?,
        ~sink: Io.Pipe.writer(string),
        ~body: Body.Make(Io).t=?,
        ~credentials: Credentials.t=?,
        ~headers: list((string, string)),
        ~meth: [ | `GET | `PUT | `POST | `DELETE | `HEAD],
        ~path: string,
        ~query: list((string, string)),
        unit
      ) =>
      Deferred.Or_error.t((int, string, Headers.t(string), string));
  };

