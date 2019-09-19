
type meth = [ | `DELETE | `GET | `HEAD | `POST | `PUT];

module Make:
  (Io: Types.Io) =>
   {
    open Io;

    let string_of_method: meth => string;

    let call:
      (
        ~expect: bool=?,
        ~endpoint: Region.endpoint,
        ~path: string,
        ~query: list((string, string))=?,
        ~headers: Headers.t(string),
        ~sink: Io.Pipe.writer(string),
        ~body: Pipe.reader(string)=?,
        meth
      ) =>
      Deferred.Or_error.t((int, string, Headers.t(string), string));
  };

