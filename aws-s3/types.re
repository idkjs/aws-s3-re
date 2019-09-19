/** Module for abstracting async and lwt */
module type Io = {
  module Deferred: {
    type t('a);
    module Or_error: {
      type nonrec t('a) = t(result('a, exn));
      let return: 'a => t('a);
      let fail: exn => t('a);
      let catch: (unit => t('a)) => t('a);
      let (>>=): (t('a), 'a => t('b)) => t('b);
    };

    let return: 'a => t('a);
    let after: float => t(unit);
    let catch: (unit => t('a)) => Or_error.t('a);
    let async: t(unit) => unit;

    let (>>=): (t('a), 'a => t('b)) => t('b);
    let (>>|): (t('a), 'a => 'b) => t('b);
    let (>>=?):
      (t(result('a, 'c)), 'a => t(result('b, 'c))) => t(result('b, 'c));
  };


  module Ivar: {
    type t('a);
    let create: unit => t('a);
    let fill: (t('a), 'a) => unit;
    let wait: t('a) => Deferred.t('a);
  };


  /** Module mimicking Async.Pipe */

  module Pipe: {
    /** Generic pipe */

    type pipe('a, 'b);


    type writer_phantom;
    type reader_phantom;


    type writer('a) = pipe('a, writer_phantom);
    type reader('a) = pipe('a, reader_phantom);

    /** Create a reader given a function f that fills the reader. Once f completes, the reader is closed */

    let create_reader: (~f: writer('a) => Deferred.t(unit)) => reader('a);

    /** Create a writer given a function f that reads off the writer. Once f completes, the writer is closed */

    let create_writer: (~f: reader('a) => Deferred.t(unit)) => writer('a);

    /** Create a reader/writer pipe. Data written to the reader can be read by the writer.
        Closing one end will close both ends. */

    let create: unit => (reader('a), writer('a));

    /** Flush a writer. The result we be determined once all elements in the pipe has been consumed */

    let flush: writer('a) => Deferred.t(unit);

    /** Write to a writer. If the writer is closed, the function raises an exception */

    let write: (writer('a), 'a) => Deferred.t(unit);

    /** Close a writer */

    let close: writer('a) => unit;

    /** Close a reader */

    let close_reader: reader('a) => unit;

    /** Read one element from a reader. The function will block until an element becomes available or the
        reader is closed, in which case [None] is returned */

    let read: reader('a) => Deferred.t(option('a));

    /** Transfer all data from the reader to the writer. The function becomes determined when the reader or writer is closed */

    let transfer: (reader('a), writer('a)) => Deferred.t(unit);

    /** Return the state of a pipe */

    let is_closed: pipe('a, 'b) => bool;

    /** Wait for a pipe to be closed. The function is determined once the pipe is closed.
        the function can be called multiple times.

        Note that not all elements may have been consumed yet.
    */

    let closed: pipe('a, 'b) => Deferred.t(unit);
  };


  module Net: {
    let connect:
      (
        ~inet: [ | `V4 | `V6],
        ~host: string,
        ~port: int,
        ~scheme: [< | `Http | `Https]
      ) =>
      Deferred.Or_error.t((Pipe.reader(string), Pipe.writer(string)));
  };

};
