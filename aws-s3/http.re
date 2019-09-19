
open StdLabels;
let sprintf = Printf.sprintf;
let debug = false;
let log = fmt =>
  debug
    ? Printf.kfprintf(_ => (), stderr, "%s: " ^^ fmt ^^ "\n%!", __MODULE__)
    : Printf.ikfprintf(_ => (), stderr, fmt);

type meth = [ | `DELETE | `GET | `HEAD | `POST | `PUT];

module Make = (Io: Types.Io) => {
  module Body = Body.Make(Io);
  open Io;
  open Deferred;

  let string_of_method =
    fun
    | `GET => "GET"
    | `PUT => "PUT"
    | `HEAD => "HEAD"
    | `POST => "POST"
    | `DELETE => "DELETE";

  let read_status = (~start=?, reader) => {
    let remain = start;
    /* Start reading the reply */
    Body.read_until(~start=?remain, ~sep=" ", reader)
    >>=? (
      ((_http_version, remain)) =>
        Body.read_until(~start=?remain, ~sep=" ", reader)
        >>=? (
          ((status_code, remain)) =>
            Body.read_until(~start=?remain, ~sep="\r\n", reader)
            >>=? (
              ((status_message, remain)) =>
                Or_error.return((
                  (int_of_string(status_code), status_message),
                  remain,
                ))
            )
        )
    );
  };

  let read_headers = (~start=?, reader) => {
    let rec inner = (~start=?, acc) =>
      Body.read_until(~start?, ~sep="\r\n", reader)
      >>=? (
        fun
        | ("", remain) => Or_error.return((acc, remain))
        | (line, remain) => {
            let (key, value) =
              switch (Str.split(Str.regexp(": "), line)) {
              | [] => failwith("Illegal header")
              | [k] => (k, "")
              | [k, v] => (k, v)
              | [k, ...vs] => (k, String.concat(~sep=": ", vs))
              };

            inner(~start=?remain, Headers.add(~key, ~value, acc));
          }
      );

    inner(~start?, Headers.empty);
  };

  let send_request = (~expect, ~path, ~query, ~headers, ~meth, writer, ()) => {
    let headers =
      expect
        ? Headers.add(~key="Expect", ~value="100-continue", headers) : headers;

    let path_with_params = {
      let query = List.map(~f=((k, v)) => (k, [v]), query);
      Uri.make(~path, ~query, ()) |> Uri.to_string;
    };

    let header =
      sprintf(
        "%s %s HTTP/1.1\r\n",
        string_of_method(meth),
        path_with_params,
      );
    Pipe.write(writer, header)
    >>= (
      () =>
        /* Write all headers */
        Headers.fold(
          (key, value, acc) =>
            acc
            >>= (
              () =>
                Pipe.write(writer, key)
                >>= (
                  () =>
                    Pipe.write(writer, ": ")
                    >>= (
                      () =>
                        Pipe.write(writer, value)
                        >>= (
                          () =>
                            Pipe.write(writer, "\r\n") >>= (() => return())
                        )
                    )
                )
            ),
          headers,
          return(),
        )
        >>= (() => Pipe.write(writer, "\r\n") >>= (() => return()))
    );
  };

  let handle_expect = (~expect, reader) =>
    expect
      ? {
        log("Expect 100-continue");
        read_status(reader)
        >>=? (
          fun
          | ((100, _), remain) => {
              log("Got 100-continue");
              Or_error.return(`Continue(remain));
            }
          | ((code, message), remain) =>
            Or_error.return(`Failed(((code, message), remain)))
        );
      }
      : Or_error.return(`Continue(None));

  let send_body = (~body=?, writer) => {
    let rec transfer = (reader, writer) =>
      Pipe.read(reader)
      >>= (
        fun
        | Some(data) =>
          Pipe.write(writer, data) >>= (() => transfer(reader, writer))
        | None => return()
      );

    switch (body) {
    | None => Or_error.return()
    | Some(reader) =>
      catch(() => transfer(reader, writer))
      >>= (
        result => {
          /* Close the reader and writer in any case */
          Pipe.close_reader(reader);
          return(result);
        }
      )
    };
  }; /* Might contain an exception */

  let read_data = (~start=?, ~sink, ~headers, reader) => {
    /* Test if the reply is chunked */
    let chunked_transfer =
      switch (Headers.find_opt("transfer-encoding", headers)) {
      | Some(encoding) =>
        List.mem("chunked", ~set=String.split_on_char(~sep=',', encoding))
      | None => false
      };

    (
      switch (Headers.find_opt("content-length", headers), chunked_transfer) {
      | (None, false) => Or_error.return(None)
      | (Some(length), false) =>
        let length = int_of_string(length);
        Body.transfer(~start?, ~length, reader, sink);
      | (_, true) =>
        /* Actually we should not accept a content
           length then when encoding is chunked, but AWS
           does require this when uploading, so we
           accept it for symmetry.*/
        Body.chunked_transfer(~start?, reader, sink)
      }
    )
    >>=? (
      _remain => {
        /* We could log here is we have extra data */
        Pipe.close(sink);
        Or_error.return();
      }
    );
  };

  let do_request =
      (
        ~expect,
        ~path,
        ~query=[],
        ~headers,
        ~sink,
        ~body=?,
        meth,
        reader,
        writer,
      ) =>
    catch(send_request(~expect, ~path, ~query, ~headers, ~meth, writer))
    >>=? (
      () =>
        handle_expect(~expect, reader)
        >>=? (
          fun
          | `Failed((code, message), remain) =>
            Or_error.return(((code, message), remain))
          | `Continue(remain) =>
            send_body(~body?, writer)
            >>=? (() => read_status(~start=?remain, reader))
        )
        >>=? (
          (((code, message), remain)) =>
            read_headers(~start=?remain, reader)
            >>=? (
              ((headers, remain)) => {
                let (error_body, error_sink) = {
                  let (reader, writer) = Pipe.create();
                  (Body.to_string(reader), writer);
                };

                (
                  switch (meth) {
                  | `HEAD => Or_error.return("")
                  | _ =>
                    let sink =
                      switch (code) {
                      | n when 200 <= n && n < 300 =>
                        Pipe.close(error_sink);
                        sink;
                      | _ =>
                        Pipe.close(sink);
                        error_sink;
                      };

                    read_data(~start=?remain, ~sink, ~headers, reader)
                    >>=? (
                      () =>
                        error_body
                        >>= (error_body => Or_error.return(error_body))
                    );
                  }
                )
                >>=? (
                  error_body =>
                    Or_error.return((code, message, headers, error_body))
                );
              }
            )
        )
    );

  let call =
      (
        ~expect=false,
        ~endpoint: Region.endpoint,
        ~path,
        ~query=[],
        ~headers,
        ~sink,
        ~body=?,
        meth: meth,
      ) =>
    Net.connect(
      ~inet=endpoint.inet,
      ~host=endpoint.host,
      ~port=endpoint.port,
      ~scheme=endpoint.scheme,
    )
    >>=? (
      ((reader, writer)) =>
        /* At this point we need to make sure reader and writer are closed properly. */
        do_request(
          ~expect,
          ~path,
          ~query,
          ~headers,
          ~sink,
          ~body?,
          meth,
          reader,
          writer,
        )
        >>= (
          result => {
            /* Close the reader and writer regardless of status */
            Pipe.close_reader(reader);
            Pipe.close(writer);
            Pipe.close(sink);
            return(result);
          }
        )
    );
};
