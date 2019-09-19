open StdLabels;
let sprintf = Printf.sprintf;
let ok_exn =
  fun
  | Ok(v) => v
  | Error(exn) => raise(exn);

module Option = {
  let value = (~default) =>
    fun
    | Some(v) => v
    | None => default;

  let value_exn = (~message) =>
    fun
    | Some(v) => v
    | None => failwith(message);
};

module Make = (Io: Aws_s3.Types.Io) => {
  module S3 = Aws_s3.S3.Make(Io);
  module Credentials = Aws_s3.Credentials.Make(Io);
  module Body = Aws_s3__Body.Make(Io);
  open Io;
  open Deferred;

  let file_length = file => {
    let ic = open_in(file);
    let len = in_channel_length(ic);
    close_in(ic);
    len;
  };

  let read_file = (~pos, ~len, file) => {
    let ic = open_in(file);
    seek_in(ic, pos);
    let data = really_input_string(ic, len);
    close_in(ic);
    data;
  };

  let file_reader = (~chunk_size=4097, ~pos, ~len, file) => {
    /* Create a stream with the data */
    let ic = open_in(file);
    /* Seek first */
    seek_in(ic, pos);
    let rec read = (len, writer) =>
      switch (len) {
      | 0 => return()
      | n when n < chunk_size =>
        let data = really_input_string(ic, n);
        Io.Pipe.write(writer, data);
      | n =>
        let data = really_input_string(ic, chunk_size);
        Io.Pipe.write(writer, data)
        >>= (
          () =>
            /* Yield */
            after(0.0) >>= (() => read(n - chunk_size, writer))
        );
      };

    let reader = Io.Pipe.create_reader(~f=read(len));
    Io.Deferred.async(
      Io.Pipe.closed(reader)
      >>= (
        () => {
          close_in(ic);
          return();
        }
      ),
    );
    reader;
  };

  let save_file = (file, contents) => {
    let oc = open_out(file);
    output_string(oc, contents);
    close_out(oc);
  };

  type objekt = {
    bucket: string,
    key: string,
  };
  let objekt_of_uri = u =>
    switch (String.split_on_char(~sep='/', u)) {
    | ["s3:", "", bucket, ...key] => {
        bucket,
        key: String.concat(~sep="/", key),
      }
    | _ => failwith("Illegal uri: " ++ u)
    };

  let string_of_error =
    fun
    | S3.Redirect(_) => "Redirect"
    | S3.Throttled => "Throttled"
    | [@implicit_arity] S3.Unknown(code, msg) =>
      sprintf("Unknown: %d, %s", code, msg)
    | S3.Failed(exn) => sprintf("Failed: %s", Printexc.to_string(exn))
    | S3.Forbidden => "Forbidden"
    | S3.Not_found => "Not_found";

  type cmd =
    | S3toLocal(objekt, string)
    | LocaltoS3(string, objekt)
    | S3toS3(objekt, objekt);

  let determine_paths = (src, dst) => {
    let is_s3 = u =>
      String.split_on_char(~sep='/', u)
      |> List.hd
      |> (scheme => scheme == "s3:");

    switch (is_s3(src), is_s3(dst)) {
    | (true, false) => [@implicit_arity] S3toLocal(objekt_of_uri(src), dst)
    | (false, true) => [@implicit_arity] LocaltoS3(src, objekt_of_uri(dst))
    | (true, true) =>
      [@implicit_arity] S3toS3(objekt_of_uri(src), objekt_of_uri(dst))
    | (false, false) => failwith("Use cp(1)")
    };
  };

  let rec upload_parts =
          (
            t,
            endpoint,
            ~retries,
            ~expect,
            ~credentials,
            ~offset=0,
            ~total,
            ~part_number=1,
            ~chunk_size=?,
            src,
          ) => {
    let f = (~size, ~endpoint, ()) =>
      switch (chunk_size) {
      | None =>
        let data = read_file(~pos=offset, ~len=size, src);
        S3.Multipart_upload.upload_part(
          ~endpoint,
          ~expect,
          ~credentials,
          t,
          ~part_number,
          ~data,
          (),
        );
      | Some(chunk_size) =>
        /* Create a reader for this section */
        let reader = file_reader(~pos=offset, ~len=size, src);
        S3.Multipart_upload.Stream.upload_part(
          ~endpoint,
          ~expect,
          ~credentials,
          t,
          ~part_number,
          ~data=reader,
          ~chunk_size,
          ~length=size,
          (),
        );
      };

    switch (total - offset) {
    | 0 => []
    | len =>
      let size = min(len, 5 * 1024 * 1024);
      [
        (S3.retry(~endpoint, ~retries, ~f=f(~size)))(),
        ...upload_parts(
             t,
             endpoint,
             ~retries,
             ~expect,
             ~credentials,
             ~offset=offset + size,
             ~total,
             ~part_number=part_number + 1,
             ~chunk_size?,
             src,
           ),
      ];
    };
  };

  let cp =
      (
        profile,
        endpoint,
        ~retries,
        ~expect,
        ~use_multi=false,
        ~first=?,
        ~last=?,
        ~chunk_size=?,
        src,
        dst,
      ) => {
    let range = {S3.first, last};
    Credentials.Helper.get_credentials(~profile?, ())
    >>= (
      credentials => {
        let credentials = ok_exn(credentials);
        switch (determine_paths(src, dst)) {
        | [@implicit_arity] S3toLocal(src, dst) =>
          let f = (~endpoint, ()) =>
            switch (chunk_size) {
            | None =>
              S3.get(
                ~endpoint,
                ~credentials,
                ~range,
                ~bucket=src.bucket,
                ~key=src.key,
                (),
              )
            | Some(_) =>
              let (body, data) = {
                let (r, w) = Pipe.create();
                (Body.to_string(r), w);
              };

              S3.Stream.get(
                ~endpoint,
                ~credentials,
                ~range,
                ~bucket=src.bucket,
                ~key=src.key,
                ~data,
                (),
              )
              >>=? (() => body >>= (body => Deferred.return(Ok(body))));
            };

          S3.retry(~endpoint, ~retries, ~f, ())
          >>=? (
            data => {
              save_file(dst, data);
              Deferred.return(Ok());
            }
          );
        | [@implicit_arity] LocaltoS3(src, dst) when use_multi =>
          let offset =
            switch (first) {
            | None => 0
            | Some(n) => n
            };

          let last =
            switch (last) {
            | None => file_length(src)
            | Some(n) => n
            };

          S3.retry(
            ~endpoint,
            ~retries,
            ~f=
              (~endpoint, ()) =>
                S3.Multipart_upload.init(
                  ~endpoint,
                  ~credentials,
                  ~bucket=dst.bucket,
                  ~key=dst.key,
                  (),
                ),
            (),
          )
          >>=? (
            t => {
              let uploads =
                upload_parts(
                  t,
                  endpoint,
                  ~retries,
                  ~expect,
                  ~chunk_size?,
                  ~credentials,
                  ~offset,
                  ~total=last,
                  src,
                );
              List.fold_left(
                ~init=Deferred.return(Ok()),
                ~f=(acc, x) => acc >>=? (() => x),
                uploads,
              )
              >>=? S3.retry(~endpoint, ~retries, ~f=(~endpoint, ()) =>
                     S3.Multipart_upload.complete(
                       ~endpoint,
                       ~credentials,
                       t,
                       (),
                     )
                   )
              >>=? (_md5 => Deferred.return(Ok()));
            }
          );
        | [@implicit_arity] LocaltoS3(src, dst) =>
          let pos = Option.value(~default=0, first);
          let len =
            switch (last) {
            | None => file_length(src) - pos
            | Some(l) => l - pos
            };

          let f =
            switch (chunk_size) {
            | None =>
              let data = read_file(~pos, ~len, src);
              (
                (~endpoint, ()) =>
                  S3.put(
                    ~endpoint,
                    ~expect,
                    ~credentials,
                    ~bucket=dst.bucket,
                    ~key=dst.key,
                    ~data,
                    (),
                  )
              );
            | Some(chunk_size) =>
              let reader = file_reader(~pos, ~len, src);
              (
                (~endpoint, ()) =>
                  S3.Stream.put(
                    ~endpoint,
                    ~expect,
                    ~credentials,
                    ~bucket=dst.bucket,
                    ~key=dst.key,
                    ~data=reader,
                    ~chunk_size,
                    ~length=len,
                    (),
                  )
              );
            };

          S3.retry(~endpoint, ~retries, ~f, ())
          >>=? (_etag => Deferred.return(Ok()));
        | [@implicit_arity] S3toS3(src, dst) =>
          S3.retry(
            ~endpoint,
            ~retries,
            ~f=
              (~endpoint, ()) =>
                S3.Multipart_upload.init(
                  ~endpoint,
                  ~credentials,
                  ~bucket=dst.bucket,
                  ~key=dst.key,
                  (),
                ),
            (),
          )
          >>=? (
            t =>
              S3.retry(
                ~endpoint,
                ~retries,
                ~f=
                  (~endpoint, ()) =>
                    S3.Multipart_upload.copy_part(
                      ~endpoint,
                      ~credentials,
                      t,
                      ~bucket=src.bucket,
                      ~key=src.key,
                      ~part_number=1,
                      (),
                    ),
                (),
              )
              >>=? S3.retry(~endpoint, ~retries, ~f=(~endpoint, ()) =>
                     S3.Multipart_upload.complete(
                       ~endpoint,
                       ~credentials,
                       t,
                       (),
                     )
                   )
              >>=? (_md5 => Deferred.return(Ok()))
          )
        };
      }
    );
  };

  let rm = (profile, endpoint, ~retries, bucket, paths) =>
    Credentials.Helper.get_credentials(~profile?, ())
    >>= (
      credentials => {
        let credentials = ok_exn(credentials);
        switch (paths) {
        | [key] =>
          S3.retry(
            ~endpoint,
            ~retries,
            ~f=S3.delete(~credentials, ~bucket, ~key),
            (),
          )
        | keys =>
          let objects: list(S3.Delete_multi.objekt) = (
            List.map(~f=key => {S3.Delete_multi.key, version_id: None}, keys):
              list(S3.Delete_multi.objekt)
          );
          S3.retry(
            ~endpoint,
            ~retries,
            ~f=S3.delete_multi(~credentials, ~bucket, ~objects),
            (),
          )
          >>=? (_deleted => Deferred.return(Ok()));
        };
      }
    );

  let head = (profile, endpoint, ~retries, path) =>
    Credentials.Helper.get_credentials(~profile?, ())
    >>= (
      credentials => {
        let credentials = ok_exn(credentials);
        let {bucket, key} = objekt_of_uri(path);
        S3.retry(
          ~endpoint,
          ~retries,
          ~f=S3.head(~credentials, ~bucket, ~key),
          (),
        )
        >>= (
          fun
          | Ok({S3.key, etag, size, _}) => {
              Printf.printf("Key: %s, Size: %d, etag: %s\n", key, size, etag);
              Deferred.return(Ok());
            }
          | Error(_) as e => Deferred.return(e)
        );
      }
    );

  let ls =
      (
        profile,
        endpoint,
        ~retries,
        ~ratelimit=?,
        ~start_after=?,
        ~max_keys=?,
        ~prefix=?,
        bucket,
      ) => {
    let ratelimit_f =
      switch (ratelimit) {
      | None => (() => Deferred.return(Ok()))
      | Some(n) => (
          () => after(1000. /. float(n)) >>= (() => Deferred.return(Ok()))
        )
      };

    let string_of_time = time =>
      Ptime.of_float_s(time)
      |> (
        fun
        | None => "<Error>"
        | Some(t) => Ptime.to_rfc3339(~space=false, t)
      );

    let rec ls_all = (~max_keys=?, (result, cont)) => {
      List.iter(
        ~f=
          ({S3.last_modified, key, size, etag, _}) =>
            Caml.Printf.printf(
              "%s\t%d\t%s\t%s\n%!",
              string_of_time(last_modified),
              size,
              key,
              etag,
            ),
        result,
      );
      let max_keys =
        switch (max_keys) {
        | Some(n) => Some(n - List.length(result))
        | None => None
        };

      switch (cont) {
      | S3.Ls.More(continuation) =>
        ratelimit_f()
        >>=? S3.retry(~endpoint, ~retries, ~f=(~endpoint as _, ()) =>
               continuation(~max_keys?, ())
             )
        >>=? ls_all(~max_keys?)
      | S3.Ls.Done => Deferred.return(Ok())
      };
    };

    Credentials.Helper.get_credentials(~profile?, ())
    >>= (
      credentials => {
        let credentials = ok_exn(credentials);
        S3.retry(
          ~endpoint,
          ~retries,
          ~f=
            S3.ls(
              ~start_after?,
              ~continuation_token=?None,
              ~credentials,
              ~max_keys?,
              ~prefix?,
              ~bucket,
            ),
          (),
        )
        >>=? ls_all(~max_keys?);
      }
    );
  };

  let exec = (({Cli.profile, https, retries, ipv6, expect}, cmd)) => {
    let inet =
      if (ipv6) {
        `V6;
      } else {
        `V4;
      };
    let scheme =
      if (https) {
        `Https;
      } else {
        `Http;
      };
    /* TODO: Get the region from the CLI */
    let region: Aws_s3.Region.t = (Us_east_1: Aws_s3.Region.t);
    let endpoint = Aws_s3.Region.endpoint(~inet, ~scheme, region);
    (
      switch (cmd) {
      | Cli.Cp({src, dest, first, last, multi, chunk_size}) =>
        cp(
          profile,
          endpoint,
          ~retries,
          ~expect,
          ~use_multi=multi,
          ~first?,
          ~last?,
          ~chunk_size?,
          src,
          dest,
        )
      | Rm({bucket, paths}) => rm(profile, endpoint, ~retries, bucket, paths)
      | Ls({ratelimit, bucket, prefix, start_after, max_keys}) =>
        ls(
          profile,
          endpoint,
          ~retries,
          ~ratelimit?,
          ~start_after?,
          ~prefix?,
          ~max_keys?,
          bucket,
        )
      | Head({path}) => head(profile, endpoint, ~retries, path)
      }
    )
    >>= (
      fun
      | Ok(_) => return(0)
      | Error(e) => {
          Printf.eprintf("Error: %s\n%!", string_of_error(e));
          return(1);
        }
    );
  };
};
