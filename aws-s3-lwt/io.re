open Lwt.Infix;
module Deferred = {
  type t('a) = Lwt.t('a);

  module Or_error = {
    type nonrec t('a) = Lwt_result.t('a, exn);
    let return = Lwt_result.return;
    let fail = Lwt_result.fail;
    let catch: (unit => t('a)) => t('a) = (
      f => Lwt.catch(f, Lwt_result.fail): (unit => t('a)) => t('a)
    );

    let (>>=) = (a, b) =>
      a
      >>= (
        fun
        | Ok(v) => b(v)
        | Error(_) as err => Lwt.return(err)
      );
  };

  let (>>=) = Lwt.Infix.(>>=);
  let (>>|) = (a, b) => a >>= (v => Lwt.return(b(v)));
  let (>>=?) = Lwt_result.Infix.(>>=);

  let return = Lwt.return;
  let after = delay => Lwt_unix.sleep(delay);
  let catch = f =>
    Lwt.catch(() => f() >>= Or_error.return, exn => Or_error.fail(exn));
  let async = t => Lwt.async(() => t);
};

module Ivar = {
  type t('a) = (Lwt.t('a), Lwt.u('a));
  let create = () => Lwt.wait();
  let fill = (t, v) => Lwt.wakeup_later(snd(t), v);
  let wait = t => fst(t);
};

module Pipe = {
  open Lwt.Infix;
  type elem('a) =
    | Flush(Lwt.u(unit))
    | Data('a);

  type writer_phantom = [ | `Writer];
  type reader_phantom = [ | `Reader];

  type pipe('a, 'phantom) = {
    cond: Lwt_condition.t(unit),
    queue: Queue.t(elem('a)),
    mutable closed: bool,
    closer: (Lwt.t(unit), Lwt.u(unit)),
  };

  type reader('a) = pipe('a, reader_phantom);
  type writer('a) = pipe('a, writer_phantom);

  let on_close = pipe =>
    Lwt.is_sleeping(fst(pipe.closer))
      ? Lwt.wakeup_later(snd(pipe.closer), ()) : ();

  let write = (writer, data) =>
    switch (writer.closed) {
    | false =>
      Queue.add(Data(data), writer.queue);
      if (Queue.length(writer.queue) == 1) {
        Lwt_condition.signal(writer.cond, ());
      };
      Lwt.return_unit;
    | true => failwith(__LOC__ ++ ": Closed")
    };

  let flush = writer =>
    Queue.length(writer.queue) == 0 && writer.closed
      ? Lwt.return()
      : {
        let (waiter, wakeup) = Lwt.wait();
        Queue.add(Flush(wakeup), writer.queue);
        if (Queue.length(writer.queue) == 1) {
          Lwt_condition.signal(writer.cond, ());
        };
        waiter;
      };

  let close = (writer: writer('a)) => {
    writer.closed = true;
    Lwt_condition.broadcast(writer.cond, ());
    on_close(writer);
  };

  let close_reader = (reader: reader('a)) => {
    reader.closed = true;
    Lwt_condition.broadcast(reader.cond, ());
    on_close(reader);
  };

  let rec read = (reader: reader('a)) =>
    switch (Queue.take(reader.queue)) {
    | Data(data) => Lwt.return(Some(data))
    | Flush(wakeup) =>
      Lwt.wakeup_later(wakeup, ());
      read(reader);
    | exception Queue.Empty when reader.closed => Lwt.return(None)
    | exception Queue.Empty =>
      Lwt_condition.wait(reader.cond) >>= (() => read(reader))
    };

  let create: unit => (reader('a), writer('a)) = (
    () => {
      let pipe = {
        cond: Lwt_condition.create(),
        queue: Queue.create(),
        closed: false,
        closer: Lwt.wait(),
      };

      (pipe, pipe);
    }:
      unit => (reader('a), writer('a))
  );

  let create_reader: (~f: writer('a) => Lwt.t(unit)) => reader('a) = (
    (~f) => {
      let (reader, writer) = create();
      Lwt.async(() =>
        Lwt.catch(
          () => f(writer),
          _ => {
            Printf.eprintf("Create_reader raised\n%!");
            Lwt.return();
          },
        )
        >>= (
          () => {
            close_reader(reader);
            Lwt.return();
          }
        )
      );
      reader;
    }:
      (~f: writer('a) => Lwt.t(unit)) => reader('a)
  );

  let create_writer: (~f: reader('a) => Lwt.t(unit)) => writer('a) = (
    (~f) => {
      let (reader, writer) = create();
      Lwt.async(() =>
        Lwt.catch(
          () => f(reader),
          _ => {
            Printf.eprintf("Create_writer raised\n%!");
            Lwt.return();
          },
        )
        >>= (
          () => {
            close(writer);
            Lwt.return();
          }
        )
      );
      writer;
    }:
      (~f: reader('a) => Lwt.t(unit)) => writer('a)
  );

  let is_closed = pipe => pipe.closed;
  let closed = pipe => fst(pipe.closer);

  /* If the writer is closed, so is the reader */
  let rec transfer = (reader, writer) =>
    is_closed(writer)
      ? {
        Printf.eprintf("Writer closed early\n%!");
        close_reader(reader);
        Lwt.return();
      }
      : (
        switch (Queue.take(reader.queue)) {
        | v =>
          Queue.push(v, writer.queue);
          if (Queue.length(writer.queue) == 1) {
            Lwt_condition.signal(writer.cond, ());
          };
          transfer(reader, writer);
        | exception Queue.Empty when reader.closed => Lwt.return()
        | exception Queue.Empty =>
          Lwt_condition.wait(reader.cond)
          >>= (() => transfer(reader, writer))
        }
      );
};

module Net = {
  let (>>=?) = Lwt_result.Infix.(>>=);
  let lookup = (~domain, host) =>
    Lwt_unix.(
      getaddrinfo(host, "", [AI_FAMILY(domain), AI_SOCKTYPE(SOCK_STREAM)])
      >>= (
        fun
        | [{ai_addr: [@implicit_arity] ADDR_INET(addr, _), _}, ..._] =>
          Deferred.Or_error.return(addr)
        | _ =>
          Deferred.Or_error.fail(
            failwith("Failed to resolve host: " ++ host),
          )
      )
    );

  let connect = (~inet, ~host, ~port, ~scheme) => {
    let domain: Lwt_unix.socket_domain = (
      switch (inet) {
      | `V4 => PF_INET
      | `V6 => PF_INET6
      }: Lwt_unix.socket_domain
    );

    lookup(~domain, host)
    >>=? (
      addr => {
        let addr = Ipaddr_unix.of_inet_addr(addr);
        let endp =
          switch (scheme) {
          | `Http => `TCP((`IP(addr), `Port(port)))
          | `Https => `TLS((`Hostname(host), `IP(addr), `Port(port)))
          };

        Conduit_lwt_unix.connect(~ctx=Conduit_lwt_unix.default_ctx, endp)
        >>= (
          ((_flow, ic, oc)) => {
            /*  Lwt_io.input_channel */
            let (reader, input) = Pipe.create();
            let buffer_size = Lwt_io.buffer_size(ic);
            let rec read = () =>
              Lwt_result.catch(Lwt_io.read(~count=buffer_size, ic))
              >>= (
                data =>
                  switch (input.Pipe.closed, data) {
                  | (_, Ok(""))
                  | (_, Error(_)) =>
                    Pipe.close(input);
                    Lwt.return();
                  | (true, _) => Lwt.return()
                  | (false, Ok(data)) =>
                    Pipe.write(input, data) >>= (() => read())
                  }
              );

            /* We close input and output when input is closed */
            Lwt.async(() =>
              Pipe.closed(reader) >>= (() => Lwt_io.close(oc))
            );
            Lwt.async(read);

            let (output, writer) = Pipe.create();

            let rec write = () =>
              switch (Queue.take(output.Pipe.queue)) {
              | Flush(waiter) =>
                Lwt_io.flush(oc)
                >>= (
                  () => {
                    Lwt.wakeup_later(waiter, ());
                    write();
                  }
                )
              | Data(data) => Lwt_io.write(oc, data) >>= (() => write())
              | exception Queue.Empty when output.Pipe.closed => Lwt.return()
              | exception Queue.Empty =>
                Lwt_condition.wait(output.Pipe.cond) >>= (() => write())
              };

            Lwt.async(write);
            Deferred.Or_error.return((reader, writer));
          }
        );
      }
    );
  };
};
