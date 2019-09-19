open Async;
module Deferred = {
  type t('a) = Async_kernel.Deferred.t('a);

  module Or_error = {
    type nonrec t('a) = t(result('a, exn));
    let return = v => Async_kernel.Deferred.return(Ok(v));
    let fail = exn => Async_kernel.Deferred.return(Error(exn));
    let catch = f =>
      Async_kernel.Monitor.try_with(f)
      >>= (
        fun
        | Ok(v) => Async_kernel.return(v)
        | Error(exn) => Async_kernel.return(Error(exn))
      );

    let (>>=): (t('a), 'a => t('b)) => t('b) = (
      (v, f) =>
        v
        >>= (
          fun
          | Ok(v) => f(v)
          | Error(exn) => Async_kernel.return(Error(exn))
        ):
        (t('a), 'a => t('b)) => t('b)
    );
  };

  let (>>=) = Async_kernel.(>>=);
  let (>>|) = Async_kernel.(>>|);
  let (>>=?) = (v, f) =>
    v
    >>= (
      fun
      | Ok(v) => f(v)
      | Error(exn) => return(Error(exn))
    );

  let return = Async_kernel.return;
  let after = delay =>
    Async_kernel.after(Core_kernel.Time_ns.Span.of_sec(delay));
  let catch = f => Async_kernel.Monitor.try_with(f);
  let async = don't_wait_for;
};

module Ivar = {
  type t('a) = Async.Ivar.t('a);
  let create = () => Async.Ivar.create();
  let fill = (t, v) => Async.Ivar.fill(t, v);
  let wait = t => Async.Ivar.read(t);
};

module Pipe = {
  open Async_kernel;
  open Deferred;

  type pipe('a, 'b) = Pipe.pipe('a, 'b);
  type writer_phantom = Pipe.Writer.phantom;
  type reader_phantom = Pipe.Reader.phantom;
  type writer('a) = Pipe.Writer.t('a);
  type reader('a) = Pipe.Reader.t('a);

  let flush = writer => Pipe.downstream_flushed(writer) >>= (_ => return());
  let read = reader =>
    Pipe.read(reader)
    >>= (
      fun
      | `Eof => return(None)
      | `Ok(v) => return(Some(v))
    );
  let write = (writer, data) => {
    /* Pipe.write writer data */
    Pipe.write_without_pushback(writer, data);
    return();
  };
  let close = writer => Pipe.close(writer);
  let close_reader = reader => Pipe.close_read(reader);
  let create_reader = (~f) =>
    Pipe.create_reader(~close_on_exception=true, f);
  let create_writer = (~f) => Pipe.create_writer(f);
  let transfer = (reader, writer) => Pipe.transfer_id(reader, writer);
  let create = () => Pipe.create();
  let is_closed = pipe => Pipe.is_closed(pipe);
  let closed = pipe => Pipe.closed(pipe);
};

module Net = {
  let lookup = (~domain, host) =>
    Async_unix.Unix.(
      Addr_info.get(
        ~host,
        [Addr_info.AI_FAMILY(domain), Addr_info.AI_SOCKTYPE(SOCK_STREAM)],
      )
      >>= (
        fun
        | [
            {Addr_info.ai_addr: [@implicit_arity] ADDR_INET(addr, _), _},
            ..._,
          ] =>
          Deferred.Or_error.return(addr)
        | _ =>
          Deferred.Or_error.fail(
            failwith("Failed to resolve host: " ++ host),
          )
      )
    );

  let connect = (~inet, ~host, ~port, ~scheme) => {
    let domain: Async_unix.Unix.socket_domain = (
      switch (inet) {
      | `V4 => PF_INET
      | `V6 => PF_INET6
      }: Async_unix.Unix.socket_domain
    );

    lookup(~domain, host)
    >>=? (
      addr => {
        let ip_addr = Ipaddr_unix.of_inet_addr(addr);
        let endp =
          switch (scheme) {
          | `Http => `TCP((ip_addr, port))
          | `Https => `OpenSSL((host, ip_addr, port))
          };

        Conduit_async.connect(endp)
        >>= (
          ((ic, oc)) => {
            let reader = Reader.pipe(ic);
            don't_wait_for(
              Async_kernel.Pipe.closed(reader)
              >>= (() => Reader.close(ic) >>= (() => Writer.close(oc))),
            );
            let writer = Writer.pipe(oc);
            Deferred.Or_error.return((reader, writer));
          }
        );
      }
    );
  };
};
