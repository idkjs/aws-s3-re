module Make:
  (Io: Types.Io) =>
   {
    open Io;
    type t =
      | String(string)
      | Empty
      | Chunked{
          pipe: Pipe.reader(string),
          length: int,
          chunk_size: int,
        };

    let to_string: Pipe.reader(string) => Deferred.t(string);

    let read_string:
      (~start: string=?, ~length: int, Pipe.reader(string)) =>
      Deferred.Or_error.t((string, option(string)));

    let read_until:
      (~start: string=?, ~sep: string, Pipe.reader(string)) =>
      Deferred.Or_error.t((string, option(string)));

    let chunked_transfer:
      (~start: string=?, Pipe.reader(string), Pipe.writer(string)) =>
      Deferred.Or_error.t(option(string));

    let transfer:
      (
        ~start: string=?,
        ~length: int,
        Pipe.reader(string),
        Pipe.writer(string)
      ) =>
      Deferred.Or_error.t(option(string));

    let null: unit => Pipe.writer(string);

  };
