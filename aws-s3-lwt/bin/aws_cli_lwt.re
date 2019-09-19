module Aws = Aws_cli.Aws.Make(Aws_s3_lwt.Io);

let exec = cmd => Lwt_main.run(Aws.exec(cmd));

let () = Aws_cli.Cli.parse(exec);
