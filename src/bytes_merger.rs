use bytes::{BufMut, Bytes, BytesMut};

pub fn buffered_bytes_channel(buffer_count: usize, threshold_size: usize)
    -> (tokio::sync::mpsc::Sender<Bytes>, tokio::sync::mpsc::Receiver<Bytes>) {

    let (input_tx, mut input_rx) = tokio::sync::mpsc::channel::<Bytes>(buffer_count);
    let (output_tx, output_rx) = tokio::sync::mpsc::channel::<Bytes>(buffer_count);

    tokio::spawn(async move {
        let mut vec = Vec::<Bytes>::new();

        loop {
            match input_rx.recv().await {
                Some(chunk) => {
                    // empty Bytes means an EOF flag, mark it
                    let chunk_is_empty = chunk.is_empty();
                    // push onto buffer if not EOF flag
                    if !chunk_is_empty {
                        vec.push(chunk)
                    }

                    if chunk_is_empty || total_byte_length(&vec) >= threshold_size {
                        // Merge several Bytes element into one
                        if vec.len() > 1 {
                            merge_vec_bytes(&mut vec)
                        };

                        // if the chunk is an EOF flag, push an empty bytes
                        if chunk_is_empty {
                            vec.push(Bytes::new());
                        }

                        for bytes in vec.drain(..) {
                            if output_tx.send(bytes).await.is_err() {
                                tracing::trace!("Output receiver dropped, exit");
                                return;
                            }
                        }
                    }
                },
                None => {
                    tracing::trace!("Input channel exited, exit");

                    // If there's bytes remained here, flush them
                    if !vec.is_empty() && total_byte_length(&vec) > 0 {
                        tracing::trace!("Try Flushing buffered chunks");
                        merge_vec_bytes(&mut vec);
                        for bytes in vec.drain(..) {
                            let _ = output_tx.send(bytes).await;
                        }
                    }

                    drop(output_tx);
                    break;
                }
            }
        }
    });

    (input_tx, output_rx)
}

fn total_byte_length(vec: &Vec<Bytes>) -> usize {
    vec.iter()
        .map(|b| b.len())
        .sum()
}

fn merge_vec_bytes(vec: &mut Vec<Bytes>) {
    let mut big_buffer = BytesMut::with_capacity(total_byte_length(vec));
    for buf in vec.drain(..) {
        big_buffer.put(buf);
    }
    vec.push(big_buffer.freeze());
}
