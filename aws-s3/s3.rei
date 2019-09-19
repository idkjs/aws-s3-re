/** S3 functions
    All function requires a [region], [scheme] and [credentials].

    The default region is [Us_east_1].

    The default scheme is [http]. If you are connecting from outside AWS,
    it is strongly recommended that you use https.
    To use https, make sure to have the relevant opam packages installed:
    [async_ssl] for [async] and [lwt_ssl]/[tls] for [lwt].
    Please note that connections are not reused due to a limitation on the AWS endpoint.


    If no credentials is provided, the requests will not be signed,
    The bucket / objects need to be configured accordingly.

    IPv6 connection can be set globally using the function:
    {!Aws_s3.S3.Make.set_connection_type}.
*/

module Make:
  (Io: Types.Io) =>
   {
    open Io;

    type error =
      | Redirect(Region.endpoint)
      | Throttled
      | Unknown(int, string)
      | Failed(exn)
      | Forbidden
      | Not_found;

    type etag = string;
    type storage_class =
      | Standard
      | Standard_ia
      | Onezone_ia
      | Reduced_redundancy
      | Glacier;
    type content = {
      storage_class,
      size: int,
      /** Seconds since epoch */
      last_modified: float,
      key: string,
      /** Etag as a string. this us usually the MD5, unless the object was constructed by multi-upload */
      etag,
    };

    type nonrec result('a) = Deferred.t(result('a, error));
    type command('a) =
      (~credentials: Credentials.t=?, ~endpoint: Region.endpoint) => 'a;

    module Ls: {
      type t = result((list(content), cont))
      and cont =
        | More((~max_keys: int=?, unit) => t)
        | Done;
    };

    module Delete_multi: {
      type objekt = {
        key: string,
        version_id: option(string),
      };
      type error = {
        key: string,
        version_id: option(string),
        code: string,
        message: string,
      };
      type result = {
        delete_marker: bool,
        delete_marker_version_id: option(string),
        deleted: list(objekt),
        error: list(error),
      };
    };

    type range = {
      first: option(int),
      last: option(int),
    };

    /** Upload [data] to [bucket]/[key].
      Returns the etag of the object. The etag is the md5 checksum (RFC 1864)

      @param expect If true, the body will not be sent until a
      status has been received from the server. This incurs a delay
      in transfer, but avoid sending a large body, if the request can be
      know to fail before the body is sent.
  */

    let put:
      command(
        (
          ~content_type: string=?,
          ~content_encoding: string=?,
          ~acl: string=?,
          ~cache_control: string=?,
          ~expect: bool=?,
          ~bucket: string,
          ~key: string,
          ~data: string,
          unit
        ) =>
        result(etag),
      );

    /** Download [key] from s3 in [bucket]
      If [range] is specified, only a part of the file is retrieved:
      - If [first] is None, then start from the beginning of the object.
      - If [last] is None, then get to the end of the object.
  */

    let get:
      command(
        (~range: range=?, ~bucket: string, ~key: string, unit) =>
        result(string),
      );

    /** Call head on the object to retrieve info on a single object */

    let head:
      command((~bucket: string, ~key: string, unit) => result(content));

    /** Delete [key] from [bucket]. */

    let delete:
      command((~bucket: string, ~key: string, unit) => result(unit));

    /** Delete multiple objects from [bucket].

      The result will indicate which items failed and which are deleted. If
      an item is not found it will be reported as successfully deleted
      (the operation is idempotent).
  */

    let delete_multi:
      command(
        (~bucket: string, ~objects: list(Delete_multi.objekt), unit) =>
        result(Delete_multi.result),
      );

    /** List contents in [bucket]
      Aws will return at most 1000 keys per request. If not all keys are
      returned, the function will return a continuation.

      Keys in s3 are stored in lexicographical order, and also returned as such.

      If a [continuation_token] is given the result will continue from last call.

      If [start_after] is given then keys only keys after start_with are returned.
      Note that is both [start_after] and a [continuation_token] is given
      then start_after argument is ignored.

      If prefix is given, then only keys starting with the given prefix will be returned.
  */

    let ls:
      command(
        (
          ~start_after: string=?,
          ~continuation_token: string=?,
          ~prefix: string=?,
          ~max_keys: int=?,
          ~bucket: string,
          unit
        ) =>
        Ls.t,
      );

    /** Streaming functions.
      Streaming function seeks to limit the amount of used memory used when
      operating of large objects by operating on streams.
  */

    module Stream: {
      /** Streaming version of put.
        @param length Amount of data to copy
        @param chunk_size The size of chunks send to s3.
               The system will have 2 x chunk_size byte in flight
        @param data stream to be uploaded. Data will not be consumed after
               the result is determined. If using [expect], then data may not have been consumed at all,
               but it is up to the caller to test if data has been consumed from the input data.

        see {!Aws_s3.S3.Make.put}
    */

      let put:
        command(
          (
            ~content_type: string=?,
            ~content_encoding: string=?,
            ~acl: string=?,
            ~cache_control: string=?,
            ~expect: bool=?,
            ~bucket: string,
            ~key: string,
            ~data: Io.Pipe.reader(string),
            ~chunk_size: int,
            ~length: int,
            unit
          ) =>
          result(etag),
        );

      /** Streaming version of get.
        The caller must supply a [data] sink to which retrieved data is streamed.
        The result will be determined after all data has been sent to the sink, and the data sink is closed.

        Connections to s3 is closed once the result has been determined.
        The caller should ways examine the result of the function.
        If the result is [Ok ()], then it is guaranteed that all data has been retrieved successfully and written to the data sink.
        In case of [Error _], only parts of the data may have been written to the data sink.

        The rationale for using a data sink rather than returning a pipe reader from which data
        can be consumed is that a reader does not allow simple relay of error states during the transfer.

        For other parameters see {!Aws_s3.S3.Make.get}
    */

      let get:
        command(
          (
            ~range: range=?,
            ~bucket: string,
            ~key: string,
            ~data: Io.Pipe.writer(string),
            unit
          ) =>
          result(unit),
        );
    };

    module Multipart_upload: {
      type t;

      /** Initialize multipart upload */

      let init:
        command(
          (
            ~content_type: string=?,
            ~content_encoding: (string, string)=?,
            ~acl: string=?,
            ~cache_control: string=?,
            ~bucket: string,
            ~key: string,
            unit
          ) =>
          result(t),
        );

      /** Upload a part of the file. All parts except the last part must
       be at least 5Mb big. All parts must have a unique part number.
       The final file will be assembled from all parts ordered by part
       number

       @param expect: If true, the body will not be sent until a
       tatus has been received from the server. This incurs a delay
       in transfer, but avoid sending a large body, if the request is
       know to fail before the body is sent.
    */

      let upload_part:
        command(
          (t, ~part_number: int, ~expect: bool=?, ~data: string, unit) =>
          result(unit),
        );

      /** Specify a part as a copy of an existing object in S3. */

      let copy_part:
        command(
          (
            t,
            ~part_number: int,
            ~range: (int, int)=?,
            ~bucket: string,
            ~key: string,
            unit
          ) =>
          result(unit),
        );

      /** Complete a multipart upload. The returned string is an opaque identifier used as etag.
        the etag return is _NOT_ the md5 */

      let complete: command((t, unit) => result(etag));

      /** Abort a multipart upload. This also discards all uploaded parts. */

      let abort: command((t, unit) => result(unit));

      /** Streaming functions */

      module Stream: {
        /** Streaming version of upload_part.
          @param length is the amount of data to copy
          @param chunk_size Is the size of chunks send to s3.
                 The system will have 2 x chunk_size byte in flight
          @param data the streamed data. Data will not be consumed after
                 the result is determined. If using [expect], then data may not have been consumed at all,
                 but it is up to the caller to test if data has been consumed from the input data.

          see {!Aws_s3.S3.Make.Multipart_upload.upload_part}
      */

        let upload_part:
          command(
            (
              t,
              ~part_number: int,
              ~expect: bool=?,
              ~data: Io.Pipe.reader(string),
              ~length: int,
              ~chunk_size: int,
              unit
            ) =>
            result(unit),
          );
      };
    };

    /** Helper function to handle error codes.
      The function handle redirects and throttling.
  */

    let retry:
      (
        ~endpoint: Region.endpoint,
        ~retries: int,
        ~f: (~endpoint: Region.endpoint, unit) => result('a),
        unit
      ) =>
      result('a);
  };
