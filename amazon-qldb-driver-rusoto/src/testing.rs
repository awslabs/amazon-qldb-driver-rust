use async_trait::async_trait;
use rusoto_core::RusotoError;
use rusoto_qldb_session::*;
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

/// A mock implementation of [`QldbSession`]. The client can be cloned to
/// retain access to the testing functionality, even after the instance is
/// handed to a [`QldbDriver`].
///
/// The client generates default responses (literally using the [`Default`]
/// trait), but provides a facility to override the response per request.
///
/// Creating the request matchers and response types is a little tedious, so
/// we also provide some factory methods to make things easier.
#[derive(Clone)]
pub struct TestQldbSessionClient {
    inner: Arc<TestQldbSessionClientInner>,
}

/// This typealias gives a name to something that looks a little scary.
/// Essentially, this holds overrides to the default responses.
///
/// Probably want a map in the future so it's easy to update responses, but
/// that requires dealing with the fact that the rusoto types don't
/// implement Hash.
type RequestResponseStore = Vec<(
    SendCommandRequest,
    Result<SendCommandResult, RusotoError<SendCommandError>>,
)>;

pub fn build_response(
    req: SendCommandRequest,
) -> Result<SendCommandResult, RusotoError<SendCommandError>> {
    match req {
        SendCommandRequest {
            abort_transaction: Some(abort),
            ..
        } => Ok(build_abort_transaction_response(abort)),
        SendCommandRequest {
            commit_transaction: Some(commit),
            ..
        } => Ok(build_commit_transaction_response(commit)),
        SendCommandRequest {
            end_session: Some(end),
            ..
        } => Ok(build_end_transaction_response(end)),
        SendCommandRequest {
            execute_statement: Some(execute),
            ..
        } => Ok(build_execute_statement_response(execute)),
        SendCommandRequest {
            fetch_page: Some(fetch_page),
            ..
        } => Ok(build_fetch_page_response(fetch_page)),
        SendCommandRequest {
            start_session: Some(start_session),
            ..
        } => Ok(build_start_session_response(start_session)),
        SendCommandRequest {
            start_transaction: Some(start_transaction),
            ..
        } => Ok(build_start_transaction_response(start_transaction)),
        _ => unreachable!(),
    }
}

pub fn build_abort_transaction_response(_req: AbortTransactionRequest) -> SendCommandResult {
    SendCommandResult {
        abort_transaction: Some(AbortTransactionResult {
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_commit_transaction_response(req: CommitTransactionRequest) -> SendCommandResult {
    SendCommandResult {
        commit_transaction: Some(CommitTransactionResult {
            transaction_id: Some(req.transaction_id),
            commit_digest: Some(req.commit_digest),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_end_transaction_response(_req: EndSessionRequest) -> SendCommandResult {
    SendCommandResult {
        end_session: Some(EndSessionResult {
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_execute_statement_response(_req: ExecuteStatementRequest) -> SendCommandResult {
    SendCommandResult {
        execute_statement: Some(ExecuteStatementResult {
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_fetch_page_response(_req: FetchPageRequest) -> SendCommandResult {
    SendCommandResult {
        fetch_page: Some(FetchPageResult {
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_start_session_response(req: StartSessionRequest) -> SendCommandResult {
    let now = SystemTime::now();

    SendCommandResult {
        start_session: Some(StartSessionResult {
            session_token: Some(format!(
                "session-{}-{}",
                req.ledger_name,
                now.duration_since(UNIX_EPOCH).unwrap().as_millis()
            )),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn build_start_transaction_response(_req: StartTransactionRequest) -> SendCommandResult {
    let now = SystemTime::now();

    SendCommandResult {
        start_transaction: Some(StartTransactionResult {
            transaction_id: Some(format!(
                "transaction-{}",
                now.duration_since(UNIX_EPOCH).unwrap().as_millis()
            )),
            ..Default::default()
        }),
        ..Default::default()
    }
}

// This is not a very good mock, but it'll do. Each request variant has a
// queue of responses.
//
// We use a Mutex for interior mutability, since `send_command` takes
// `&self`.
struct TestQldbSessionClientInner {
    queue: Mutex<RequestResponseStore>,
}

impl TestQldbSessionClient {
    pub fn new() -> TestQldbSessionClient {
        TestQldbSessionClient {
            inner: Arc::new(TestQldbSessionClientInner {
                queue: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn respond(
        &mut self,
        req: SendCommandRequest,
        with: Result<SendCommandResult, RusotoError<SendCommandError>>,
    ) {
        let mut queue = self.inner.queue.lock().unwrap();
        queue.push((req, with));
    }
}

impl Default for TestQldbSessionClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QldbSession for TestQldbSessionClient {
    async fn send_command(
        &self,
        req: SendCommandRequest,
    ) -> Result<SendCommandResult, RusotoError<SendCommandError>> {
        let mut queue = self.inner.queue.lock().unwrap();
        match queue.iter().position(|it| it.0 == req) {
            Some(i) => {
                let res = queue.remove(i);
                res.1
            }
            None => build_response(req),
        }
    }
}
