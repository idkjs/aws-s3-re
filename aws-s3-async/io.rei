include
  Aws_s3.Types.Io with
    type Deferred.t('a) = Async_kernel.Deferred.t('a) and
    type Pipe.pipe('a, 'b) = Async_kernel.Pipe.t('a, 'b) and
    type Pipe.reader_phantom = Async_kernel.Pipe.Reader.phantom and
    type Pipe.writer_phantom = Async_kernel.Pipe.Writer.phantom and
    type Pipe.reader('a) = Async_kernel.Pipe.Reader.t('a) and
    type Pipe.writer('a) = Async_kernel.Pipe.Writer.t('a);
