use crate::errors::FragmentationProcessorError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc;

pub struct StreamProcessor<T> {
    pub(crate) stream: TcpStream,
    pub(crate) fragmentation_processor:
        fn(&[u8]) -> (Result<T, FragmentationProcessorError>, usize),
    pub(crate) frame_deserialization: fn(&T) -> &[u8],
}
impl<T: Send + 'static> StreamProcessor<T> {
    pub fn frame_provider(
        mut self,
    ) -> (
        mpsc::Receiver<Result<T, FragmentationProcessorError>>,
        mpsc::Sender<T>,
    ) {
        let (rx, tx) = mpsc::channel::<Result<T, FragmentationProcessorError>>(512);
        let (write_rx, write_tx) = mpsc::channel::<T>(5);

        tokio::spawn(async move {
            let rx = rx;
            let mut wr_tx = write_tx;
            let mut buff: Vec<u8> = Vec::new();
            let mut frame = [0u8; 10240];

            loop {
                select! {
                    frame = wr_tx.recv() => {
                        let frame = frame.unwrap();
                        let bytes = (self.frame_deserialization)(&frame);
                        let r = self.stream.write(bytes).await;
                        if r.is_err() {
                            eprintln!("error: {:?}", r.err());
                            let _ = rx.send(Err(FragmentationProcessorError::ConnectionClosed)).await;
                            return ;
                        }
                    },
                    stream_result = self.stream.read(&mut frame) => {
                            if let Ok(len) = stream_result {
                            if len == 0 {
                                let _ = rx.send(Err(FragmentationProcessorError::ConnectionClosed)).await;
                                return ;
                            } else {
                                    buff.extend_from_slice(&frame[0..len]);
                                    let (result, sz) = (self.fragmentation_processor)(buff.as_slice());
                                    match result {
                                        Ok(fragment) => {
                                            let _ = rx.send(Ok(fragment)).await;
                                        }
                                        Err(_) => {

                                        continue;
                                    }
                                }
                                buff.drain(0..sz);
                            }
                        }
                    },


                };
            }
        });
        return (tx, write_rx);
    }
}
