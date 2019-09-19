/** Parse command line options */
open Cmdliner;

type actions =
  | Ls{
      bucket: string,
      prefix: option(string),
      start_after: option(string),
      ratelimit: option(int),
      max_keys: option(int),
    }
  | Head{path: string}
  | Rm{
      bucket: string,
      paths: list(string),
    }
  | Cp{
      src: string,
      dest: string,
      first: option(int),
      last: option(int),
      multi: bool,
      chunk_size: option(int),
    };

type options = {
  profile: option(string),
  https: bool,
  retries: int,
  ipv6: bool,
  expect: bool,
};

let parse = exec => {
  let profile = {
    let doc = "Specify profile to use.";
    Arg.(
      value
      & opt(some(string), None)
      & info(["profile", "p"], ~docv="PROFILE", ~doc)
    );
  };

  let ratelimit = {
    let doc = "Limit requests to N/sec.";
    Arg.(
      value
      & opt(some(int), None)
      & info(["ratelimit", "r"], ~docv="LIMIT", ~doc)
    );
  };

  let https = {
    let doc = "Enable/disable https.";
    Arg.(value & opt(bool, false) & info(["https"], ~docv="HTTPS", ~doc));
  };

  let ipv6 = {
    let doc = "Use ipv6";
    Arg.(value & flag & info(["6"], ~docv="IPV6", ~doc));
  };

  let retries = {
    let doc = "Retries in case of error";
    Arg.(value & opt(int, 0) & info(["retries"], ~docv="RETIES", ~doc));
  };

  let expect = {
    let doc = "Use expect -> 100-continue for put/upload_chunk";
    Arg.(value & flag & info(["expect", "e"], ~docv="EXPECT", ~doc));
  };

  let common_opts = {
    let make = (profile, https, retries, ipv6, expect) => {
      profile,
      https,
      retries,
      ipv6,
      expect,
    };
    Term.(const(make) $ profile $ https $ retries $ ipv6 $ expect);
  };

  let bucket = n => {
    let doc = "S3 bucket name";
    Arg.(
      required & pos(n, some(string), None) & info([], ~docv="BUCKET", ~doc)
    );
  };

  let path = (n, name) => {
    let doc = "path: <local_path>|s3://<bucket>/<objname>";
    Arg.(
      required & pos(n, some(string), None) & info([], ~docv=name, ~doc)
    );
  };

  let cp = {
    let make = (opts, first, last, multi, chunk_size, src, dest) => (
      opts,
      Cp({src, dest, first, last, multi, chunk_size}),
    );

    let first = {
      let doc = "first byte of the source object to copy. If omitted means from the start.";
      Arg.(
        value
        & opt(some(int), None)
        & info(["first", "f"], ~docv="FIRST", ~doc)
      );
    };

    let last = {
      let doc = "last byte of the source object to copy. If omitted means to the end";
      Arg.(
        value
        & opt(some(int), None)
        & info(["last", "l"], ~docv="LAST", ~doc)
      );
    };

    let multi = {
      let doc = "Use multipart upload";
      Arg.(value & flag & info(["multi", "m"], ~docv="MULTI", ~doc));
    };

    let chunk_size = {
      let doc = "Use streaming get / put the given chunk_size";
      Arg.(
        value
        & opt(some(int), None)
        & info(["chunk-size", "c"], ~docv="CHUNK SIZE", ~doc)
      );
    };

    (
      Term.(
        const(make)
        $ common_opts
        $ first
        $ last
        $ multi
        $ chunk_size
        $ path(0, "SRC")
        $ path(1, "DEST")
      ),
      Term.info("cp", ~doc="Copy files to and from S3"),
    );
  };

  let rm = {
    let objects = {
      let doc = "name of the object to delete";
      Arg.(
        non_empty & pos_right(0, string, []) & info([], ~docv="OBJECT", ~doc)
      );
    };

    let make = (opts, bucket, paths) => (opts, Rm({bucket, paths}));
    (
      Term.(const(make) $ common_opts $ bucket(0) $ objects),
      Term.info("rm", ~doc="Delete files from s3"),
    );
  };

  let head = {
    let path = {
      let doc = "object: s3://<bucket>/<objname>";
      Arg.(
        required & pos(0, some(string), None) & info([], ~docv="PATH", ~doc)
      );
    };

    let make = (opts, path) => (opts, Head({path: path}));
    (
      Term.(const(make) $ common_opts $ path),
      Term.info("head", ~doc="Head files from s3"),
    );
  };

  let ls = {
    let make = (opts, ratelimit, prefix, start_after, bucket, max_keys) => (
      opts,
      Ls({bucket, prefix, start_after, ratelimit, max_keys}),
    );

    let prefix = {
      let doc = "Only list elements with the given prefix";
      Arg.(
        value
        & opt(some(string), None)
        & info(["prefix"], ~docv="PREFIX", ~doc)
      );
    };

    let max_keys = {
      let doc = "Max keys returned per ls request";
      Arg.(
        value
        & opt(some(int), None)
        & info(["max-keys"], ~docv="MAX KEYS", ~doc)
      );
    };

    let start_after = {
      let doc = "List objects after the given key";
      Arg.(
        value
        & opt(some(string), None)
        & info(["start-after"], ~docv="START AFTER", ~doc)
      );
    };

    (
      Term.(
        const(make)
        $ common_opts
        $ ratelimit
        $ prefix
        $ start_after
        $ bucket(0)
        $ max_keys
      ),
      Term.info("ls", ~doc="List files in bucket"),
    );
  };

  /* Where do the result go? */
  let help = {
    let doc = "Amazon s3 command line interface";
    let exits = Term.default_exits;
    (
      Term.(ret(const(_ => `Help((`Pager, None))) $ common_opts)),
      Term.info(Sys.argv[0], ~doc, ~exits),
    );
  };

  let commands = {
    let cmds = [cp, rm, ls, head];
    Term.(eval_choice(help, cmds));
  };

  let run =
    fun
    | `Ok(cmd) => exec(cmd)
    | _ => 254;

  run @@ commands |> exit;
};
