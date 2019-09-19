open StdLabels;
let sprintf = Printf.sprintf;
let debug = false;
let log = fmt =>
  debug
    ? Printf.kfprintf(_ => (), stderr, "%s: " ^^ fmt ^^ "\n%!", __MODULE__)
    : Printf.ikfprintf(_ => (), stderr, fmt);
let _ = log;

let empty_sha = Authorization.hash_sha256("") |> Authorization.to_hex;

let get_chunked_length = (~chunk_size, payload_length) => {
  let lengths =
    (sprintf("%x", chunk_size) |> String.length)
    * (payload_length / chunk_size)
    + (
      switch (payload_length mod chunk_size) {
      | 0 => 0
      | n => sprintf("%x", n) |> String.length
      }
    );

  let chunks = (payload_length + (chunk_size - 1)) / chunk_size + 1;
  chunks * (4 + 17 + 64) + lengths + 1 + payload_length;
};

let%test "get_chunk_length" =
  get_chunked_length(~chunk_size=64 * 1024, 65 * 1024) == 66824;

module Make = (Io: Types.Io) => {
  module Body = Body.Make(Io);
  module Http = Http.Make(Io);
  open Io;
  open Deferred;

  let chunk_writer =
      (
        ~signing_key,
        ~scope,
        ~initial_signature,
        ~date,
        ~time,
        ~chunk_size,
        reader,
      ) => {
    open Deferred;
    let sub = (~pos, ~len=?, str) => {
      let res =
        switch (pos, len) {
        | (0, None) => str
        | (0, Some(len)) when len == String.length(str) => str
        | (pos, None) =>
          String.sub(~pos, ~len=String.length(str) - pos, str)
        | (pos, Some(len)) => String.sub(~pos, ~len, str)
        };

      res;
    };

    let send = (writer, sha, previous_signature, elements, length) => {
      let flush_done = Pipe.flush(writer);
      let sha = Digestif.SHA256.get(sha);
      let signature =
        Authorization.chunk_signature(
          ~signing_key,
          ~date,
          ~time,
          ~scope,
          ~previous_signature,
          ~sha,
        )
        |> Authorization.to_hex;
      Io.Pipe.write(
        writer,
        Printf.sprintf("%x;chunk-signature=%s\r\n", length, signature),
      )
      >>= (
        () =>
          List.fold_left(
            ~init=Deferred.return(),
            ~f=(x, data) => x >>= (() => Io.Pipe.write(writer, data)),
            elements,
          )
          >>= (
            () =>
              Io.Pipe.write(writer, "\r\n")
              >>= (() => flush_done >>= (() => return(signature)))
          )
      );
    };

    let rec transfer =
            (previous_signature, ctx, buffered: int, queue, current, writer) =>
      (
        switch (current) {
        | Some(v) => return(Some(v))
        | None =>
          Io.Pipe.read(reader)
          >>= (
            fun
            | None => return(None)
            | Some(v) => return(Some((v, 0)))
          )
        }
      )
      >>= (
        fun
        | None =>
          send(writer, ctx, previous_signature, List.rev(queue), buffered)
          >>= (
            signature =>
              send(writer, Digestif.SHA256.empty, signature, [], 0)
              >>= (_signature => Deferred.return())
          )
        | Some((data, offset)) => {
            let remain = chunk_size - buffered;
            switch (String.length(data) - offset) {
            | n when n >= remain =>
              let elem = sub(data, ~pos=offset, ~len=remain);
              let ctx = Digestif.SHA256.feed_string(ctx, elem);
              let elements = [elem, ...queue] |> List.rev;
              send(writer, ctx, previous_signature, elements, chunk_size)
              >>= (
                signature => {
                  /* Recursive call. */
                  let data =
                    String.length(data) > remain
                      ? Some((data, offset + remain)) : None;

                  transfer(
                    signature,
                    Digestif.SHA256.empty,
                    0,
                    [],
                    data,
                    writer,
                  );
                }
              );
            | _ =>
              let elem = sub(~pos=offset, data);
              let ctx = Digestif.SHA256.feed_string(ctx, elem);
              transfer(
                previous_signature,
                ctx,
                buffered + String.length(elem),
                [elem, ...queue],
                None,
                writer,
              );
            };
          }
      );

    Pipe.create_reader(
      ~f=transfer(initial_signature, Digestif.SHA256.empty, 0, [], None),
    );
  };

  let make_request =
      (
        ~endpoint: Region.endpoint,
        ~expect=false,
        ~sink,
        ~body=Body.Empty,
        ~credentials: option(Credentials.t)=?,
        ~headers,
        ~meth,
        ~path,
        ~query,
        (),
      ) => {
    let (date, time) = Unix.gettimeofday() |> Time.iso8601_of_time;

    /* Create headers structure */
    let content_length =
      switch (meth, body) {
      | (`PUT | `POST, Body.String(body)) =>
        Some(String.length(body) |> string_of_int)
      | (`PUT | `POST, Body.Chunked({length, chunk_size, _})) =>
        Some(get_chunked_length(~chunk_size, length) |> string_of_int)
      | (`PUT | `POST, Body.Empty) => Some("0")
      | _ => None
      };

    let payload_sha =
      switch (body) {
      | Body.Empty => empty_sha
      | Body.String(body) =>
        Authorization.hash_sha256(body) |> Authorization.to_hex
      | Body.Chunked(_) => "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
      };

    let token =
      switch (credentials) {
      | Some({Credentials.token, _}) => token
      | None => None
      };

    let (headers, body) = {
      let decoded_content_length =
        switch (body) {
        | Body.Chunked({length, _}) => Some(string_of_int(length))
        | _ => None
        };

      open Headers;
      let headers =
        List.fold_left(
          ~init=empty,
          ~f=(m, (key, value)) => add(~key, ~value, m),
          headers,
        )
        |> add(~key="Host", ~value=endpoint.host)
        |> add(~key="x-amz-content-sha256", ~value=payload_sha)
        |> add(~key="x-amz-date", ~value=sprintf("%sT%sZ", date, time))
        |> add_opt(~key="x-amz-security-token", ~value=token)
        |> add_opt(~key="Content-Length", ~value=content_length)
        |> change(
             ~key="User-Agent",
             ~f=
               fun
               | Some(_) as r => r
               | None => Some("aws-s3 ocaml client"),
           )
        |> add_opt(
             ~key="x-amz-decoded-content-length",
             ~value=decoded_content_length,
           )
        |> change(~key="Content-Encoding", ~f=v =>
             switch (body, v) {
             | (Body.Chunked(_), Some(v)) => Some("aws-chunked," ++ v)
             | (Body.Chunked(_), None) => Some("aws-chunked")
             | (_, v) => v
             }
           );

      let (auth, body) =
        switch (credentials) {
        | Some(credentials) =>
          let verb = Http.string_of_method(meth);
          let region = Region.to_string(endpoint.region);
          let signing_key =
            Authorization.make_signing_key(
              ~date,
              ~region,
              ~service="s3",
              ~credentials,
              (),
            );

          let scope = Authorization.make_scope(~date, ~region, ~service="s3");
          let (signature, signed_headers) =
            Authorization.make_signature(
              ~date,
              ~time,
              ~verb,
              ~path,
              ~headers,
              ~query,
              ~scope,
              ~signing_key,
              ~payload_sha,
            );

          let auth =
            Authorization.make_auth_header(
              ~credentials,
              ~scope,
              ~signed_headers,
              ~signature,
            );
          let body =
            switch (body) {
            | Body.String(body) =>
              let (reader, writer) = Pipe.create();
              Pipe.write(writer, body)
              >>= (
                () => {
                  Pipe.close(writer);
                  return(Some(reader));
                }
              );
            | Body.Empty => return(None)
            | Body.Chunked({pipe, chunk_size, _}) =>
              let pipe =
                /* Get errors if the chunk_writer fails */
                chunk_writer(
                  ~signing_key,
                  ~scope,
                  ~initial_signature=signature,
                  ~date,
                  ~time,
                  ~chunk_size,
                  pipe,
                );

              return(Some(pipe));
            };

          (Some(auth), body);
        | None =>
          let body =
            switch (body) {
            | Body.String(body) =>
              let (reader, writer) = Pipe.create();
              Pipe.write(writer, body)
              >>= (
                () => {
                  Pipe.close(writer);
                  return(Some(reader));
                }
              );
            | Body.Empty => return(None)
            | Body.Chunked({pipe, _}) => return(Some(pipe))
            };

          (None, body);
        };

      (Headers.add_opt(~key="Authorization", ~value=auth, headers), body);
    };

    body
    >>= (
      body =>
        Http.call(
          ~endpoint,
          ~path,
          ~query,
          ~headers,
          ~expect,
          ~sink,
          ~body?,
          meth,
        )
        >>=? (
          ((code, msg, headers, body)) =>
            Deferred.Or_error.return((code, msg, headers, body))
        )
    );
  };
};
