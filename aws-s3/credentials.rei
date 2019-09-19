/** Loading credentials locally or from IAM service. */
type t = {
  access_key: string,
  secret_key: string,
  token: option(string),
  expiration: option(float),
};

/** Make credentials */

let make:
  (
    ~access_key: string,
    ~secret_key: string,
    ~token: string=?,
    ~expiration: float=?,
    unit
  ) =>
  t;

module Make:
  (Io: Types.Io) =>
   {
    open Io;

    module Iam: {
      /** Get machine role though IAM service */

      let get_role: unit => Deferred.Or_error.t(string);

      /** Retrieve a credentials for a given role [role] */

      let get_credentials: string => Deferred.Or_error.t(t);
    };

    module Local: {
      /** Load credentials from ~/.aws/credentials (file format compatible
        with botocore). */

      let get_credentials:
        (~profile: string=?, unit) => Deferred.Or_error.t(t);
    };

    module Helper: {
      /** Get credentials locally or though IAM service.
        [profile] is used to speficy a specific section thethe local file.

        If profile is not supplied and no credentials can be found in
        the default section, then credentials are retrieved though Iam
        service, using an assigned machine role.
    */

      let get_credentials:
        (~profile: string=?, unit) => Deferred.Or_error.t(t);
    };
  };
