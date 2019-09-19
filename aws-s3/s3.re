/*{{{
 * Copyright (C) 2015 Trevor Smith <trevorsummerssmith@gmail.com>
 * Copyright (C) 2017 Anders Fugmann <anders@fugmann.net>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
  }}}*/
open StdLabels;
let sprintf = Printf.sprintf;
open Protocol_conv_xml;

module Option = {
  let map = (~default=?, ~f) =>
    fun
    | None => default
    | Some(v) => Some(f(v));

  let value = (~default) =>
    fun
    | None => default
    | Some(v) => v;

  let value_map = (~default, ~f, v) => map(~f, v) |> value(~default);

  let value_exn = (~message) =>
    fun
    | Some(v) => v
    | None => failwith(message);
};

let rec filter_map = (~f) =>
  fun
  | [] => []
  | [x, ...xs] =>
    switch (f(x)) {
    | Some(x) => [x, ...filter_map(~f, xs)]
    | None => filter_map(~f, xs)
    };

/* Protocol definitions */
module Protocol = (P: {type result('a);}) => {
  type time = float;
  let time_of_xml_light_exn = t =>
    try (Xml_light.to_string(t) |> Time.parse_iso8601_string) {
    | _ =>
      raise(
        Xml_light.(
          Protocol_error(make_error(~value=t, "Not an iso8601 string"))
        ),
      )
    };

  let unquote = s =>
    switch (String.length(s)) {
    | 0
    | 1 => s
    | _ when s.[0] == '"' => String.sub(s, ~pos=1, ~len=String.length(s) - 2)
    | _ => s
    };

  type etag = string;
  let etag_of_xml_light_exn = t => Xml_light.to_string(t) |> unquote;

  type storage_class =
    | [@key "STANDARD"] Standard
    | [@key "STANDARD_IA"] Standard_ia
    | [@key "ONEZONE_IA"] Onezone_ia
    | [@key "REDUCED_REDUNDANCY"] Reduced_redundancy
    | [@key "GLACIER"] Glacier
  [@deriving of_protocol(~driver=(module Xml_light))]
  and content = {
    [@key "StorageClass"]
    storage_class,
    [@key "Size"]
    size: int,
    [@key "LastModified"]
    last_modified: time,
    [@key "Key"]
    key: string,
    /** Add expiration date option */
    [@key "ETag"]
    etag,
  };

  module Ls = {
    [@deriving of_protocol(~driver=(module Xml_light))]
    type result = {
      [@key "Prefix"]
      prefix: option(string),
      [@key "CommonPrefixes"]
      common_prefixes: option(string),
      [@key "Delimiter"]
      delimiter: option(string),
      [@key "NextContinuationToken"]
      next_continuation_token: option(string),
      [@key "Name"]
      name: string,
      [@key "MaxKeys"]
      max_keys: int,
      [@key "KeyCount"]
      key_count: int,
      [@key "IsTruncated"]
      is_truncated: bool,
      [@key "Contents"]
      contents: list(content),
      [@key "StartAfter"]
      start_after: option(string),
    };

    type t = P.result((list(content), cont))
    and cont =
      | More((~max_keys: int=?, unit) => t)
      | Done;
  };

  let set_element_name = name =>
    fun
    | [@implicit_arity] Xml.Element(_, attribs, ts) =>
      [@implicit_arity] Xml.Element(name, attribs, ts)
    | _ => failwith("Not an element");

  module Delete_multi = {
    [@deriving of_protocol(~driver=(module Xml_light))]
    type objekt = {
      [@key "Key"]
      key: string,
      [@key "VersionId"]
      version_id: option(string),
    };

    /** We must not transmit the version id at all if not specified */

    let objekt_to_xml_light =
      fun
      | {key, version_id: None} =>
        [@implicit_arity]
        Xml.Element(
          "object",
          [],
          [[@implicit_arity] Xml.Element("Key", [], [Xml.PCData(key)])],
        )
      | {key, version_id: Some(version)} =>
        [@implicit_arity]
        Xml.Element(
          "object",
          [],
          [
            [@implicit_arity] Xml.Element("Key", [], [Xml.PCData(key)]),
            [@implicit_arity]
            Xml.Element("VersionId", [], [Xml.PCData(version)]),
          ],
        );

    [@deriving to_protocol(~driver=(module Xml_light))]
    type request = {
      [@key "Quiet"]
      quiet: bool,
      [@key "Object"]
      objects: list(objekt),
    };

    let xml_of_request = request =>
      request_to_xml_light(request) |> set_element_name("Delete");

    [@deriving of_protocol(~driver=(module Xml_light))]
    type error = {
      [@key "Key"]
      key: string,
      [@key "VersionId"]
      version_id: option(string),
      [@key "Code"]
      code: string,
      [@key "Message"]
      message: string,
    };

    type delete_marker = bool;

    let delete_marker_of_xml_light_exn = t =>
      Xml_light.to_option(Xml_light.to_bool, t)
      |> (
        fun
        | None => false
        | Some(x) => x
      );

    [@deriving of_protocol(~driver=(module Xml_light))]
    type result = {
      [@key "DeleteMarker"]
      delete_marker,
      [@key "DeleteMarkerVersionId"]
      delete_marker_version_id: option(string),
      [@key "Deleted"]
      deleted: list(objekt),
      [@key "Error"]
      error: list(error),
    };
  };

  module Error_response = {
    [@deriving of_protocol(~driver=(module Xml_light))]
    type t = {
      [@key "Code"]
      code: string,
      [@key "Message"]
      message: string,
      [@key "Bucket"]
      bucket: option(string),
      [@key "Endpoint"]
      endpoint: option(string),
      [@key "Region"]
      region: option(string),
      [@key "RequestId"]
      request_id: string,
      [@key "HostId"]
      host_id: string,
    };
  };

  module Multipart = {
    [@deriving to_protocol(~driver=(module Xml_light))]
    type part = {
      [@key "PartNumber"]
      part_number: int,
      [@key "ETag"]
      etag: string,
    };

    module Initiate = {
      [@deriving of_protocol(~driver=(module Xml_light))]
      type t = {
        [@key "Bucket"]
        bucket: string,
        [@key "Key"]
        key: string,
        [@key "UploadId"]
        upload_id: string,
      };
    };
    module Complete = {
      [@deriving to_protocol(~driver=(module Xml_light))]
      type request = {
        [@key "Part"]
        parts: list(part),
      };
      let xml_of_request = request =>
        request_to_xml_light(request)
        |> set_element_name("CompleteMultipartUpload");
      [@deriving of_protocol(~driver=(module Xml_light))]
      type response = {
        [@key "Location"]
        location: string,
        [@key "Bucket"]
        bucket: string,
        [@key "Key"]
        key: string,
        [@key "ETag"]
        etag: string,
      };
    };
    module Copy = {
      [@deriving of_protocol(~driver=(module Xml_light))]
      type t = {
        [@key "ETag"]
        etag: string,
        [@key "LastModified"]
        last_modified: time,
      };
    };
  };
};

module Make = (Io: Types.Io) => {
  module Aws = Aws.Make(Io);
  module Body = Body.Make(Io);
  open Io;
  open Deferred;
  type error =
    | Redirect(Region.endpoint)
    | Throttled
    | Unknown(int, string)
    | Failed(exn)
    | Forbidden
    | Not_found;

  let string_sink = () => {
    let (reader, writer) = Pipe.create();
    (Body.to_string(reader), writer);
  };

  include Protocol({
    type nonrec result('a) = Deferred.t(result('a, error));
  });

  type range = {
    first: option(int),
    last: option(int),
  };

  type nonrec result('a) = Deferred.t(result('a, error));
  type command('a) =
    (~credentials: Credentials.t=?, ~endpoint: Region.endpoint) => 'a;


  let do_command = (~endpoint: Region.endpoint, cmd) =>
    cmd()
    >>= (
      fun
      | Ok(v) => return(Ok(v))
      | Error(exn) => return(Error(Failed(exn)))
    )
    >>=? (
      ((code, _message, headers, body)) =>
        switch (code) {
        | code when 200 <= code && code < 300 => Deferred.return(Ok(headers))
        | 403 => Deferred.return(Error(Forbidden))
        | 404 => Deferred.return(Error(Not_found))
        | c when 300 <= c && c < 400 =>
          /* Redirect of sorts */
          let region =
            Region.of_string(Headers.find("x-amz-bucket-region", headers));

          Deferred.return(Error(Redirect({...endpoint, region})));
        | c when 400 <= c && c < 500 =>
          open Error_response;
          let xml = Xml.parse_string(body);
          switch (Error_response.of_xml_light_exn(xml)) {
          | {code: "PermanentRedirect", endpoint: Some(host), _}
          | {code: "TemporaryRedirect", endpoint: Some(host), _} =>
            let region = Region.of_host(host);
            Deferred.return(Error(Redirect({...endpoint, region})));
          | {code: "AuthorizationHeaderMalformed", region: Some(region), _} =>
            let region = Region.of_string(region);
            Deferred.return(Error(Redirect({...endpoint, region})));
          | {code, _} =>
            Deferred.return(Error([@implicit_arity] Unknown(c, code)))
          };
        | 500
        | 503 =>
          /* 500, InternalError | 503, SlowDown | 503, ServiceUnavailable -> Throttle */
          Deferred.return(Error(Throttled))
        | code =>
          let resp = Error_response.of_xml_light_exn(Xml.parse_string(body));
          Deferred.return(
            Error([@implicit_arity] Unknown(code, resp.code)),
          );
        }
    );

  let put_common =
      (
        ~credentials=?,
        ~endpoint,
        ~content_type=?,
        ~content_encoding=?,
        ~acl=?,
        ~cache_control=?,
        ~expect=?,
        ~bucket,
        ~key,
        ~body,
        (),
      ) => {
    let path = sprintf("/%s/%s", bucket, key);
    let headers =
      [
        ("Content-Type", content_type),
        ("Content-Encoding", content_encoding),
        ("Cache-Control", cache_control),
        ("x-amz-acl", acl),
      ]
      |> List.filter(
           ~f=
             fun
             | (_, Some(_)) => true
             | (_, None) => false,
         )
      |> List.map(
           ~f=
             fun
             | (k, Some(v)) => (k, v)
             | (_, None) => failwith("Impossible"),
         );

    let sink = Body.null();
    let cmd = () =>
      Aws.make_request(
        ~endpoint,
        ~expect?,
        ~credentials?,
        ~headers,
        ~meth=`PUT,
        ~path,
        ~sink,
        ~body,
        ~query=[],
        (),
      );

    do_command(~endpoint, cmd)
    >>=? (
      headers => {
        let etag =
          switch (Headers.find_opt("etag", headers)) {
          | None => failwith("Put reply did not contain an etag header")
          | Some(etag) => unquote(etag)
          };

        Deferred.return(Ok(etag));
      }
    );
  };

  module Stream = {
    let get = (~credentials=?, ~endpoint, ~range=?, ~bucket, ~key, ~data, ()) => {
      let headers = {
        let r_opt =
          fun
          | Some(r) => string_of_int(r)
          | None => "";

        switch (range) {
        | Some({first: None, last: None}) => []
        | Some({first: Some(first), last}) => [
            ("Range", sprintf("bytes=%d-%s", first, r_opt(last))),
          ]
        | Some({first: None, last: Some(last)}) when last < 0 => [
            ("Range", sprintf("bytes=-%d", last)),
          ]
        | Some({first: None, last: Some(last)}) => [
            ("Range", sprintf("bytes=0-%d", last)),
          ]
        | None => []
        };
      };

      let path = sprintf("/%s/%s", bucket, key);
      let cmd = () =>
        Aws.make_request(
          ~endpoint,
          ~credentials?,
          ~sink=data,
          ~headers,
          ~meth=`GET,
          ~path,
          ~query=[],
          (),
        );

      do_command(~endpoint, cmd) >>=? (_headers => Deferred.return(Ok()));
    };

    let put =
        (
          ~credentials=?,
          ~endpoint,
          ~content_type=?,
          ~content_encoding=?,
          ~acl=?,
          ~cache_control=?,
          ~expect=?,
          ~bucket,
          ~key,
          ~data,
          ~chunk_size,
          ~length,
          (),
        ) => {
      let body = Body.Chunked({length, chunk_size, pipe: data});
      put_common(
        ~endpoint,
        ~credentials?,
        ~content_type?,
        ~content_encoding?,
        ~acl?,
        ~cache_control?,
        ~expect?,
        ~bucket,
        ~key,
        ~body,
        (),
      );
    };
  };
  /* End streaming module */

  let put =
      (
        ~credentials=?,
        ~endpoint,
        ~content_type=?,
        ~content_encoding=?,
        ~acl=?,
        ~cache_control=?,
        ~expect=?,
        ~bucket,
        ~key,
        ~data,
        (),
      ) => {
    let body = Body.String(data);
    put_common(
      ~credentials?,
      ~content_type?,
      ~content_encoding?,
      ~acl?,
      ~cache_control?,
      ~expect?,
      ~endpoint,
      ~bucket,
      ~key,
      ~body,
      (),
    );
  };

  let get = (~credentials=?, ~endpoint, ~range=?, ~bucket, ~key, ()) => {
    let (body, data) = string_sink();
    Stream.get(~credentials?, ~range?, ~endpoint, ~bucket, ~key, ~data, ())
    >>=? (() => body >>= (body => Deferred.return(Ok(body))));
  };

  let delete = (~credentials=?, ~endpoint, ~bucket, ~key, ()) => {
    let path = sprintf("/%s/%s", bucket, key);
    let sink = Body.null();
    let cmd = () =>
      Aws.make_request(
        ~credentials?,
        ~endpoint,
        ~headers=[],
        ~meth=`DELETE,
        ~path,
        ~query=[],
        ~sink,
        (),
      );

    do_command(~endpoint, cmd) >>=? (_headers => Deferred.return(Ok()));
  };

  let head = (~credentials=?, ~endpoint, ~bucket, ~key, ()) => {
    let path = sprintf("/%s/%s", bucket, key);
    let sink = Body.null();
    let cmd = () =>
      Aws.make_request(
        ~credentials?,
        ~endpoint,
        ~headers=[],
        ~meth=`HEAD,
        ~path,
        ~query=[],
        ~sink,
        (),
      );

    do_command(~endpoint, cmd)
    >>=? (
      headers => {
        let result = {
          let (>>=) = (a, f) =>
            switch (a) {
            | Some(x) => f(x)
            | None => None
            };

          Headers.find_opt("content-length", headers)
          >>= (
            size =>
              Headers.find_opt("etag", headers)
              >>= (
                etag =>
                  Headers.find_opt("last-modified", headers)
                  >>= (
                    last_modified => {
                      let last_modified =
                        Time.parse_rcf1123_string(last_modified);
                      let size = size |> int_of_string;
                      let storage_class =
                        Headers.find_opt("x-amz-storage-class", headers)
                        |> Option.value_map(~default=Standard, ~f=s =>
                             storage_class_of_xml_light_exn(
                               [@implicit_arity]
                               Xml.Element("p", [], [Xml.PCData(s)]),
                             )
                           );

                      Some({
                        storage_class,
                        size,
                        last_modified,
                        key,
                        etag: unquote(etag),
                      });
                    }
                  )
              )
          );
        };

        switch (result) {
        | Some(r) => Deferred.return(Ok(r))
        | None =>
          Deferred.return(
            Error(
              [@implicit_arity]
              Unknown(1, "Result did not return correct headers"),
            ),
          )
        };
      }
    );
  };

  let delete_multi = (~credentials=?, ~endpoint, ~bucket, ~objects, ()) =>
    switch (objects) {
    | [] =>
      Delete_multi.{
        delete_marker: false,
        delete_marker_version_id: None,
        deleted: [],
        error: [],
      }
      |> (r => Deferred.return(Ok(r)))
    | _ =>
      let request =
        Delete_multi.{quiet: false, objects}
        |> Delete_multi.xml_of_request
        |> Xml.to_string;

      let headers = [
        ("Content-MD5", Base64.encode_string(Caml.Digest.string(request))),
      ];
      let (body, sink) = string_sink();
      let cmd = () =>
        Aws.make_request(
          ~endpoint,
          ~body=Body.String(request),
          ~credentials?,
          ~headers,
          ~meth=`POST,
          ~query=[("delete", "")],
          ~path="/" ++ bucket,
          ~sink,
          (),
        );

      do_command(~endpoint, cmd)
      >>=? (
        _headers =>
          body
          >>= (
            body => {
              let result =
                Delete_multi.result_of_xml_light_exn(Xml.parse_string(body));
              Deferred.return(Ok(result));
            }
          )
      );
    };

  /** List contents of bucket in s3. */

  let rec ls =
          (
            ~credentials=?,
            ~endpoint,
            ~start_after=?,
            ~continuation_token=?,
            ~prefix=?,
            ~max_keys=?,
            ~bucket,
            (),
          ) => {
    let max_keys =
      switch (max_keys) {
      | Some(n) when n > 1000 => None
      | n => n
      };

    let query =
      [
        Some(("list-type", "2")),
        Option.map(~f=ct => ("continuation-token", ct), continuation_token),
        Option.map(~f=prefix => ("prefix", prefix), prefix),
        Option.map(
          ~f=max_keys => ("max-keys", string_of_int(max_keys)),
          max_keys,
        ),
        Option.map(
          ~f=start_after => ("start-after", start_after),
          start_after,
        ),
      ]
      |> filter_map(~f=x => x);

    let (body, sink) = string_sink();
    let cmd = () =>
      Aws.make_request(
        ~credentials?,
        ~endpoint,
        ~headers=[],
        ~meth=`GET,
        ~path="/" ++ bucket,
        ~query,
        ~sink,
        (),
      );

    do_command(~endpoint, cmd)
    >>=? (
      _headers =>
        body
        >>= (
          body => {
            let result = Ls.result_of_xml_light_exn(Xml.parse_string(body));
            let continuation =
              switch (Ls.(result.next_continuation_token)) {
              | Some(ct) =>
                Ls.More(
                  ls(
                    ~credentials?,
                    ~start_after=?None,
                    ~continuation_token=ct,
                    ~prefix?,
                    ~endpoint,
                    ~bucket,
                  ),
                )
              | None => Ls.Done
              };

            Deferred.return(Ok(Ls.(result.contents, continuation)));
          }
        )
    );
  };

  /** Function for doing multipart uploads */
  module Multipart_upload = {
    type t = {
      id: string,
      mutable parts: list(Multipart.part),
      bucket: string,
      key: string,
    };

    /** Initiate a multipart upload */

    let init =
        (
          ~credentials=?,
          ~endpoint,
          ~content_type=?,
          ~content_encoding=?,
          ~acl=?,
          ~cache_control=?,
          ~bucket,
          ~key,
          (),
        ) => {
      let path = sprintf("/%s/%s", bucket, key);
      let query = [("uploads", "")];
      let headers = {
        let content_type =
          Option.map(~f=ct => ("Content-Type", ct), content_type);
        let cache_control =
          Option.map(~f=cc => ("Cache-Control", cc), cache_control);
        let acl = Option.map(~f=acl => ("x-amz-acl", acl), acl);
        filter_map(
          ~f=x => x,
          [content_type, content_encoding, cache_control, acl],
        );
      };

      let (body, sink) = string_sink();
      let cmd = () =>
        Aws.make_request(
          ~credentials?,
          ~endpoint,
          ~headers,
          ~meth=`POST,
          ~path,
          ~query,
          ~sink,
          (),
        );

      do_command(~endpoint, cmd)
      >>=? (
        _headers =>
          body
          >>= (
            body => {
              let resp =
                Multipart.Initiate.of_xml_light_exn(Xml.parse_string(body));
              Ok({
                id: resp.Multipart.Initiate.upload_id,
                parts: [],
                bucket,
                key,
              })
              |> Deferred.return;
            }
          )
      );
    };

    /** Upload a part of the file.
        Parts must be at least 5Mb except for the last part
        [part_number] specifies the part numer. Parts will be assembled in order, but
        does not have to be consecutive
    */

    let upload_part =
        (~credentials=?, ~endpoint, t, ~part_number, ~expect=?, ~data, ()) => {
      let path = sprintf("/%s/%s", t.bucket, t.key);
      let query = [
        ("partNumber", string_of_int(part_number)),
        ("uploadId", t.id),
      ];

      let sink = Body.null();
      let cmd = () =>
        Aws.make_request(
          ~expect?,
          ~credentials?,
          ~endpoint,
          ~headers=[],
          ~meth=`PUT,
          ~path,
          ~body=Body.String(data),
          ~query,
          ~sink,
          (),
        );

      do_command(~endpoint, cmd)
      >>=? (
        headers => {
          let etag =
            Headers.find_opt("etag", headers)
            |> (
              etag =>
                Option.value_exn(
                  ~message="Put reply did not conatin an etag header",
                  etag,
                )
            )
            |> (etag => unquote(etag));

          t.parts = [{etag, part_number}, ...t.parts];
          Deferred.return(Ok());
        }
      );
    };

    /** Specify a part to be a file on s3.
        [range] can be used to only include a part of the s3 file
    */

    let copy_part =
        (
          ~credentials=?,
          ~endpoint,
          t,
          ~part_number,
          ~range=?,
          ~bucket,
          ~key,
          (),
        ) => {
      let path = sprintf("/%s/%s", t.bucket, t.key);
      let query = [
        ("partNumber", string_of_int(part_number)),
        ("uploadId", t.id),
      ];

      let headers = [
        ("x-amz-copy-source", sprintf("/%s/%s", bucket, key)),
        ...Option.value_map(
             ~default=[],
             ~f=
               ((first, last)) =>
                 [
                   (
                     "x-amz-copy-source-range",
                     sprintf("bytes=%d-%d", first, last),
                   ),
                 ],
             range,
           ),
      ];

      let (body, sink) = string_sink();
      let cmd = () =>
        Aws.make_request(
          ~credentials?,
          ~endpoint,
          ~headers,
          ~meth=`PUT,
          ~path,
          ~query,
          ~sink,
          (),
        );

      do_command(~endpoint, cmd)
      >>=? (
        _headers =>
          body
          >>= (
            body => {
              let xml = Xml.parse_string(body);
              switch (Multipart.Copy.of_xml_light_exn(xml)) {
              | {Multipart.Copy.etag, _} =>
                t.parts = [{etag, part_number}, ...t.parts];
                Deferred.return(Ok());
              };
            }
          )
      );
    };

    /** Complete the multipart upload.
        The returned etag is a opaque identifier (not md5)
    */

    let complete = (~credentials=?, ~endpoint, t, ()) => {
      let path = sprintf("/%s/%s", t.bucket, t.key);
      let query = [("uploadId", t.id)];
      let request = {
        /* TODO: Sort the parts by partNumber */
        let parts =
          Caml.List.sort(
            (a, b) => compare(a.Multipart.part_number, b.part_number),
            t.parts,
          );
        Multipart.Complete.(xml_of_request({parts: parts}))
        |> Xml.to_string_fmt;
      };

      let (body, sink) = string_sink();
      let cmd = () =>
        Aws.make_request(
          ~credentials?,
          ~endpoint,
          ~headers=[],
          ~meth=`POST,
          ~path,
          ~query,
          ~body=Body.String(request),
          ~sink,
          (),
        );

      do_command(~endpoint, cmd)
      >>=? (
        _headers =>
          body
          >>= (
            body => {
              let xml = Xml.parse_string(body);
              switch (Multipart.Complete.response_of_xml_light_exn(xml)) {
              | {location: _, etag, bucket, key}
                  when bucket == t.bucket && key == t.key =>
                Ok(etag) |> Deferred.return
              | _ =>
                Error(
                  [@implicit_arity] Unknown(-1, "Bucket/key does not match"),
                )
                |> Deferred.return
              };
            }
          )
      );
    };

    /** Abort a multipart upload, deleting all specified parts */

    let abort = (~credentials=?, ~endpoint, t, ()) => {
      let path = sprintf("/%s/%s", t.bucket, t.key);
      let query = [("uploadId", t.id)];
      let sink = Body.null();
      let cmd = () =>
        Aws.make_request(
          ~credentials?,
          ~endpoint,
          ~headers=[],
          ~meth=`DELETE,
          ~path,
          ~query,
          ~sink,
          (),
        );

      do_command(~endpoint, cmd) >>=? (_headers => Deferred.return(Ok()));
    };

    module Stream = {
      let upload_part =
          (
            ~credentials=?,
            ~endpoint,
            t,
            ~part_number,
            ~expect=?,
            ~data,
            ~length,
            ~chunk_size,
            (),
          ) => {
        let path = sprintf("/%s/%s", t.bucket, t.key);
        let query = [
          ("partNumber", string_of_int(part_number)),
          ("uploadId", t.id),
        ];

        let body = Body.Chunked({length, chunk_size, pipe: data});
        let sink = Body.null();
        let cmd = () =>
          Aws.make_request(
            ~expect?,
            ~credentials?,
            ~endpoint,
            ~headers=[],
            ~meth=`PUT,
            ~path,
            ~body,
            ~query,
            ~sink,
            (),
          );

        do_command(~endpoint, cmd)
        >>=? (
          headers => {
            let etag =
              Headers.find_opt("etag", headers)
              |> (
                etag =>
                  Option.value_exn(
                    ~message="Put reply did not conatin an etag header",
                    etag,
                  )
              )
              |> (
                etag =>
                  String.sub(~pos=1, ~len=String.length(etag) - 2, etag)
              );

            t.parts = [{etag, part_number}, ...t.parts];
            Deferred.return(Ok());
          }
        );
      };
    };
  };

  let retry = (~endpoint, ~retries, ~f, ()) => {
    let delay = n => {
      let jitter = Random.float(0.5) +. 0.5;
      let backoff = 2.0 ** float(n);
      min(60.0, backoff) *. jitter;
    };

    let rec inner = (~endpoint, ~retry_count, ~redirected, ()) =>
      f(~endpoint, ())
      >>= (
        fun
        | Error(Redirect(_)) as e when redirected => Deferred.return(e)
        | Error(Redirect(endpoint)) =>
          inner(~endpoint, ~retry_count, ~redirected=true, ())
        | Error(_) as e when retry_count == retries => Deferred.return(e)
        | Error(Throttled) =>
          Deferred.after(delay(retry_count + 1))
          >>= (
            () =>
              inner(~endpoint, ~retry_count=retry_count + 1, ~redirected, ())
          )
        | Error(_) =>
          inner(~endpoint, ~retry_count=retry_count + 1, ~redirected, ())
        | Ok(r) => Deferred.return(Ok(r))
      );

    inner(~endpoint, ~retry_count=0, ~redirected=false, ());
  };
};

let%test _ = {
  module Protocol =
    Protocol({
      type result('a) = 'a;
    });
  let data = {|
      <ListBucketResult>
        <Name>s3_osd</Name>
        <Prefix></Prefix>
        <KeyCount>1</KeyCount>
        <MaxKeys>1000</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <Contents>
          <StorageClass>STANDARD</StorageClass>
          <Key>test</Key>
          <LastModified>2018-02-27T13:39:35.000Z</LastModified>
          <ETag>"7538d2bd85ea5dfb689ed65a0f60a7aa"</ETag>
          <Size>20</Size>
        </Contents>
        <Contents>
          <StorageClass>STANDARD</StorageClass>
          <Key>test</Key>
          <LastModified>2018-02-27T13:39:35.000Z</LastModified>
          <ETag>"7538d2bd85ea5dfb689ed65a0f60a7aa"</ETag>
          <Size>20</Size>
        </Contents>
      </ListBucketResult>
      |};

  let xml = Xml.parse_string(data);
  let result = Protocol.Ls.result_of_xml_light_exn(xml);
  2 == List.length(result.Protocol.Ls.contents)
  && "7538d2bd85ea5dfb689ed65a0f60a7aa"
  == List.hd(result.Protocol.Ls.contents).Protocol.etag;
};

let%test "parse Error_response.t" = {
  module Protocol =
    Protocol({
      type result('a) = 'a;
    });
  let data = {| <Error>
         <Code>PermanentRedirect</Code>
         <Message>The bucket you are attempting to access must be addressed using the specified endpoint. Please send all future requests to this endpoint.</Message>
         <Bucket>testbucket</Bucket>
         <Endpoint>testbucket.s3.amazonaws.com</Endpoint>
         <RequestId>9E23E3919C24476C</RequestId>
         <HostId>zdQmjTUli+pR+gwwhfGt2/s7VVerHquAPqgi9KpZ9OVsYhfF+9uAkkRJtxPcLCJKk2ZjzV1VV=</HostId>
       </Error>
    |};

  let xml = Xml.parse_string(data);
  let error = Protocol.Error_response.of_xml_light_exn(xml);
  "PermanentRedirect" == error.Protocol.Error_response.code;
};

let%test "parse Delete_multi.result" = {
  module Protocol =
    Protocol({
      type result('a) = 'a;
    });
  let data = {| <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
         <Error>
           <Key>test key1</Key>
           <Code>InternalError</Code>
           <Message>We encountered an internal error. Please try again.</Message>
         </Error>
         <Error>
           <Key>test key2</Key>
           <Code>InternalError</Code>
           <Message>We encountered an internal error. Please try again.</Message>
         </Error>
       </DeleteResult>
    |};

  let error =
    Protocol.Delete_multi.result_of_xml_light_exn(Xml.parse_string(data));
  2 == List.length(error.Protocol.Delete_multi.error)
  && "InternalError"
  == List.hd(error.Protocol.Delete_multi.error).Protocol.Delete_multi.code;
};
