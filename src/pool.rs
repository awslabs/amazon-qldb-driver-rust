use crate::api::{QldbSessionApi, SessionToken};
use crate::QldbError;
use futures::future::FutureExt;
use rusoto_qldb_session::*;
use std::{
    cell::RefCell,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

type Message = (SessionToken, bool);

/// A pool of sessions. The pool will hold up to a configured [`max`] number of sessions, after which requests will begin to queue. To get a session, call [`next`]. This returns a RAII
/// guard that will release the session back into the pool when it goes out of scope.
///
/// Invalid sessions should be marked as such via [`notify_invalid`], and the caller should decide if another session should be acquired.
///
/// Note this pool uses interior mutability such that [`next`] does not require a mutable reference. In turn this allows higher level layers (such as the driver's `transact` method) to not
/// require a mutable ref.
pub(crate) struct SessionPool {
    client: QldbSessionClient,
    ledger_name: String,
    max: usize,
    current: AtomicUsize,
    ch: (Sender<Message>, RefCell<Receiver<Message>>),
}

// FIXME: Make thread-safe then implement Sync (replace RefCell with Mutex or RwLock?).
impl SessionPool {
    pub(crate) fn new(client: QldbSessionClient, ledger_name: String, max: usize) -> SessionPool {
        let (sender, receiver) = channel(max);
        let receiver = RefCell::new(receiver);

        SessionPool {
            client: client,
            ledger_name: ledger_name,
            max: max,
            current: AtomicUsize::new(0),
            ch: (sender, receiver),
        }
    }

    /// Return a [`SessionHandle`] either from the pool or by creating a new session. If the maximum number of sessions have been opened, this method will introduce latency backpressure. It
    /// will propogate API errors (e.g. unable to start a new session due to limits, API throttling) to the caller.
    pub(crate) async fn next(&self) -> Result<SessionHandle, QldbError> {
        // Keep processing the queue of returned sessions until we find one that isn't marked invalid.
        loop {
            // Note that for the 'pool is full' case, we should *always* get `Some` out of this block (we use `recv`, which waits for one), while in the 'not full' case we don't wait.
            let next = if self.current.load(Ordering::SeqCst) == self.max {
                match self.ch.1.borrow_mut().recv().await {
                    Some(m) => m,
                    None => unreachable!(
                        "bug! pool is at max capacity but there are no outstanding session handles"
                    ),
                }
            } else {
                match self.ch.1.borrow_mut().recv().now_or_never() {
                    Some(Some(m)) => m,
                    None => break, // there are no more sessions in the pool
                    Some(None) => {
                        unreachable!("bug! pool recv channel should not be closed")
                    }
                }
            };

            // We found a free session. If it's valid, return it. Otherwise, increase our permits.
            match next {
                (token, true) => return Ok(self.build_handle(token)),
                (_, false) => self.current.fetch_sub(1, Ordering::SeqCst),
            };
        }

        // It should be impossible at this point to be at max sessions due to the guard right at the start of this method (if we're at max, we add delay).
        if self.current.load(Ordering::SeqCst) == self.max {
            unreachable!("bug! pool is at max sessions but wants to make a new one");
        }

        // This code runs when either there was no available session or the one that came back was invalid.
        self.current.fetch_add(1, Ordering::SeqCst);
        let session_token = self.client.start_session(self.ledger_name.clone()).await?;
        return Ok(self.build_handle(session_token));
    }

    fn build_handle(&self, session_token: SessionToken) -> SessionHandle {
        SessionHandle {
            session_token: session_token,
            invalid: false,
            notify: self.ch.0.clone(),
        }
    }
}

pub(crate) struct SessionHandle {
    pub(crate) session_token: SessionToken,
    invalid: bool,
    notify: Sender<Message>,
}

impl SessionHandle {
    pub(crate) fn notify_invalid(mut self) {
        self.invalid = true;
    }
}

impl Drop for SessionHandle {
    fn drop(&mut self) {
        // Should never fail because the channel is bounded by the same valid as the max pool size.
        if let Err(e) = self
            .notify
            .try_send((self.session_token.clone(), self.invalid))
        {
            debug!(
                "Unable to return session {} to the session pool: {}",
                self.session_token, e
            );
        }
    }
}
