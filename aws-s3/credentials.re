[@warning "-66"]
open! StdLabels;
let sprintf = Printf.sprintf;
open Protocol_conv_json;

type time = float;
let time_of_json_exn = t =>
  try (Json.to_string(t) |> Time.parse_iso8601_string) {
  | _ =>
    raise(
      Json.(Protocol_error(make_error(~value=t, "Not an iso8601 string"))),
    )
  };

let%test "time conv" =
  time_of_json_exn(`String("2018-08-06T19:26:20Z")) == 1533583580.;

[@deriving of_protocol(~driver=(module Json))]
type t = {
  [@key "AccessKeyId"]
  access_key: string,
  [@key "SecretAccessKey"]
  secret_key: string,
  [@key "Token"]
  token: option(string),
  [@key "Expiration"]
  expiration: option(time),
};

let make = (~access_key, ~secret_key, ~token=?, ~expiration=?, ()) => {
  access_key,
  secret_key,
  token,
  expiration,
};

module Make = (Io: Types.Io) => {
  module Http = Http.Make(Io);
  module Body = Body.Make(Io);
  open Io;
  open Deferred;

  module Iam = {
    let instance_data_endpoint = {
      let instance_data_host = "instance-data.ec2.internal";
      let instance_region = Region.Other(instance_data_host);
      Region.endpoint(~inet=`V4, ~scheme=`Http, instance_region);
    };
    let get_role = () => {
      let path = "/latest/meta-data/iam/security-credentials/";
      let (body, sink) = {
        let (reader, writer) = Pipe.create();
        (Body.to_string(reader), writer);
      };

      Http.call(
        ~endpoint=instance_data_endpoint,
        ~path,
        ~sink,
        ~headers=Headers.empty,
        `GET,
      )
      >>=? (
        ((status, message, _headers, error_body)) =>
          switch (status) {
          | code when code >= 200 && code < 300 =>
            body >>= (body => Deferred.Or_error.return(body))
          | _ =>
            let msg =
              sprintf(
                "Failed to get role. %s. Reponse %s",
                message,
                error_body,
              );
            Deferred.Or_error.fail(Failure(msg));
          }
      );
    };

    let get_credentials = role => {
      let path =
        sprintf("/latest/meta-data/iam/security-credentials/%s", role);
      let (body, sink) = {
        let (reader, writer) = Pipe.create();
        (Body.to_string(reader), writer);
      };

      Http.call(
        ~endpoint=instance_data_endpoint,
        ~path,
        ~sink,
        ~headers=Headers.empty,
        `GET,
      )
      >>=? (
        ((status, message, _headers, error_body)) =>
          switch (status) {
          | code when code >= 200 && code < 300 =>
            body
            >>= (
              body => {
                let json = Yojson.Safe.from_string(body);
                Deferred.Or_error.catch(() =>
                  of_json_exn(json) |> Deferred.Or_error.return
                );
              }
            )
          | _ =>
            let msg =
              sprintf(
                "Failed to get credentials. %s. Reponse %s",
                message,
                error_body,
              );
            Deferred.Or_error.fail(Failure(msg));
          }
      );
    };
  };

  module Local = {
    let get_credentials = (~profile="default", ()) => {
      let home =
        Sys.getenv_opt("HOME")
        |> (
          fun
          | Some(v) => v
          | None => "."
        );
      let creds_file = Printf.sprintf("%s/.aws/credentials", home);
      Deferred.Or_error.catch @@
      (
        () => {
          let ini = (new OcamlInifiles.inifile)(creds_file);
          let access_key = ini#getval(profile, "aws_access_key_id");
          let secret_key = ini#getval(profile, "aws_secret_access_key");
          make(~access_key, ~secret_key, ()) |> Deferred.Or_error.return;
        }
      );
    };
  };

  module Helper = {
    let get_credentials = (~profile=?, ()) =>
      switch (profile) {
      | Some(profile) => Local.get_credentials(~profile, ())
      | None =>
        Local.get_credentials(~profile="default", ())
        >>= (
          fun
          | Result.Ok(c) => Deferred.Or_error.return(c)
          | Error(_) =>
            Iam.get_role() >>=? (role => Iam.get_credentials(role))
        )
      };
  };
};
