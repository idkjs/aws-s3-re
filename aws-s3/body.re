open StdLabels;

module Make = (Io: Types.Io) => {
  open Io;
  open Deferred;

  type t =
    | String(string)
    | Empty
    | Chunked{
        pipe: Pipe.reader(string),
        length: int,
        chunk_size: int,
      };

  let null = () => {
    let rec read = reader =>
      Pipe.read(reader)
      >>= (
        fun
        | None => return()
        | Some(_) => read(reader)
      );

    Pipe.create_writer(~f=read);
  };

  let to_string = body => {
    let rec loop = acc =>
      Pipe.read(body)
      >>= (
        fun
        | Some(data) => loop([data, ...acc])
        | None => String.concat(~sep="", List.rev(acc)) |> return
      );

    loop([]);
  };

  let read_string = (~start=?, ~length, reader) => {
    let rec loop = (acc, data, remain) =>
      switch (data, remain) {
      | (data, 0) => Or_error.return((Buffer.contents(acc), data))
      | (None, remain) =>
        Pipe.read(reader)
        >>= (
          fun
          | None => Or_error.fail(Failure("EOF"))
          | data => loop(acc, data, remain)
        )
      | (Some(data), remain) when String.length(data) < remain =>
        Buffer.add_string(acc, data);
        loop(acc, None, remain - String.length(data));
      | (Some(data), remain) =>
        Buffer.add_substring(acc, data, 0, remain);
        Or_error.return((
          Buffer.contents(acc),
          Some(
            String.sub(data, ~pos=remain, ~len=String.length(data) - remain),
          ),
        ));
      };

    loop(Buffer.create(length), start, length);
  };

  let transfer = (~start=?, ~length, reader, writer) => {
    let rec loop = (writer, data, remain) =>
      switch (remain, data) {
      | (0, data) => Or_error.return(data)
      | (remain, Some(data)) =>
        switch (remain - String.length(data)) {
        | n when n >= 0 =>
          Pipe.write(writer, data) >>= (() => loop(writer, None, n))
        | _ =>
          /* Only write whats expected and discard the rest */
          Pipe.write(writer, String.sub(~pos=0, ~len=remain, data))
          >>= (() => loop(writer, None, 0))
        }
      | (remain, None) =>
        Pipe.read(reader)
        >>= (
          fun
          | None => Or_error.fail(Failure("Premature end of input"))
          | data => loop(writer, data, remain)
        )
      };

    loop(writer, start, length);
  };

  let read_until = (~start=?, ~sep, reader) => {
    let buffer = {
      let b = Buffer.create(256);
      switch (start) {
      | Some(data) =>
        Buffer.add_string(b, data);
        b;
      | None => b
      };
    };

    let rec loop = offset =>
      fun
      | sep_index when sep_index == String.length(sep) => {
          /* Found it. Return data */
          let v = Buffer.sub(buffer, 0, offset - String.length(sep));
          let remain =
            offset < Buffer.length(buffer)
              ? Some(
                  Buffer.sub(buffer, offset, Buffer.length(buffer) - offset),
                )
              : None;

          Or_error.return((v, remain));
        }
      | sep_index when offset >= Buffer.length(buffer) =>
        Pipe.read(reader)
        >>= (
          fun
          | Some(data) => {
              Buffer.add_string(buffer, data);
              loop(offset, sep_index);
            }
          | None =>
            Or_error.fail(
              Failure(
                Printf.sprintf(
                  "EOF while looking for '%d'",
                  Char.code(sep.[sep_index]),
                ),
              ),
            )
        )
      | sep_index when Buffer.nth(buffer, offset) == sep.[sep_index] =>
        loop(offset + 1, sep_index + 1)
      | sep_index =>
        /* Reset sep index. Look for the next element. */
        loop(offset - sep_index + 1, 0);

    loop(0, 0);
  };

  /** Chunked encoding
       format: <len_hex>\r\n<data>\r\n. Always ends with 0 length chunk
    */

  let chunked_transfer = (~start=?, reader, writer) => {
    let rec read_chunk = (data, remain) =>
      switch (data, remain) {
      | (data, 0) => return(Ok(data))
      | (Some(data), remain) when String.length(data) < remain =>
        Pipe.write(writer, data)
        >>= (() => read_chunk(None, remain - String.length(data)))
      | (Some(data), remain) =>
        Pipe.write(writer, String.sub(~pos=0, ~len=remain, data))
        >>= (
          () =>
            read_chunk(
              Some(
                String.sub(
                  ~pos=remain,
                  ~len=String.length(data) - remain,
                  data,
                ),
              ),
              0,
            )
        )
      | (None, _) =>
        Pipe.read(reader)
        >>= (
          fun
          | None => Or_error.fail(Failure("Premature EOF on input"))
          | v => read_chunk(v, remain)
        )
      };

    let rec read = remain =>
      read_until(~start=?remain, ~sep="\r\n", reader)
      >>=? (
        ((size_str, data)) =>
          (
            try (Scanf.sscanf(size_str, "%x", x => x) |> Or_error.return) {
            | _ => Or_error.fail(Failure("Malformed chunk: Invalid length"))
            }
          )
          >>=? (
            chunk_size =>
              switch (chunk_size) {
              | 0 =>
                read_until(~start=?data, ~sep="\r\n", reader)
                >>=? (((_, remain)) => Or_error.return(remain))
              | n =>
                read_chunk(data, n)
                >>=? (
                  data =>
                    read_string(~start=?data, ~length=2, reader)
                    >>=? (
                      fun
                      | ("\r\n", data) => read(data)
                      | (_, _data) =>
                        Or_error.fail(
                          Failure("Malformed chunk: CRLF not present"),
                        )
                    )
                )
              }
          )
      );

    read(start);
  };
};
