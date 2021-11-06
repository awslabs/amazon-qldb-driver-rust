use amazon_qldb_driver_core::{QldbDriver, QldbDriverBuilder, TransactionAttempt};
use aws_hyper::DynConnector;
use aws_sdk_qldbsessionv2::{Client, Config, Credentials, Region};
use aws_smithy_client::dvr::{Event, ReplayingConnection};
use aws_smithy_eventstream::frame::{DecodedFrame, HeaderValue, Message, MessageFrameDecoder};
use ion_c_sys::reader::IonCReader;
use ion_c_sys::result::IonCError;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::error::Error as StdError;

#[tokio::test]
async fn test_success() -> Result<(), Box<dyn std::error::Error>> {
    let (replayer, driver) = replaying_driver("us-west-2", include_str!("success.json")).await?;

    let table_names = driver
        .transact(|mut tx: TransactionAttempt<Infallible>| async {
            let table_names = tx
                .execute_statement("select name from information_schema.user_tables")
                .await?
                .buffered()
                .await?;

            tx.commit(table_names).await
        })
        .await?;

    // Validate the requests
    replayer
        .validate(&["content-type", "content-length"], validate_success_body)
        .await
        .unwrap();

    // Validate the responses
    let table_names = table_names
        .readers()
        .into_iter()
        .map(|reader| Ok(reader?.read_string()?.to_string()))
        .collect::<Result<Vec<String>, IonCError>>()?;
    assert_eq!(&["my_table"], &table_names[..]);

    Ok(())
}

async fn replaying_driver(
    region: &'static str,
    events_json: &str,
) -> Result<(ReplayingConnection, QldbDriver), Box<dyn std::error::Error>> {
    let events: Vec<Event> = serde_json::from_str(events_json).unwrap();
    let replayer = ReplayingConnection::new(events);

    let region = Region::from_static(region);
    let credentials = Credentials::from_keys("test-aki", "test-sak", None);
    let config = Config::builder()
        .region(region)
        .credentials_provider(credentials)
        .build();
    let client = Client::from_conf_conn(config, DynConnector::new(replayer.clone()));

    Ok((
        replayer,
        QldbDriverBuilder::default()
            .ledger_name("test-ledger-name")
            .build_with_client(client)
            .await?,
    ))
}

// Returned tuples are (SignedWrapperMessage, WrappedMessage).
// Some signed messages don't have payloads, so in those cases, the wrapped message will be None.
fn decode_frames(mut body: &[u8]) -> Vec<(Message, Option<Message>)> {
    let mut result = Vec::new();
    let mut decoder = MessageFrameDecoder::new();
    while let DecodedFrame::Complete(msg) = decoder.decode_frame(&mut body).unwrap() {
        let inner_msg = if msg.payload().is_empty() {
            None
        } else {
            Some(Message::read_from(msg.payload().as_ref()).unwrap())
        };
        result.push((msg, inner_msg));
    }
    result
}

fn validate_success_body(
    expected_body: &[u8],
    actual_body: &[u8],
) -> Result<(), Box<dyn StdError>> {
    validate_body(expected_body, actual_body, true)
}

// For the error test, the second request frame may not be sent by the client depending on when
// the error response is parsed and bubbled up to the user.
fn validate_error_body(expected_body: &[u8], actual_body: &[u8]) -> Result<(), Box<dyn StdError>> {
    validate_body(expected_body, actual_body, false)
}

fn validate_body(
    expected_body: &[u8],
    actual_body: &[u8],
    full_stream: bool,
) -> Result<(), Box<dyn StdError>> {
    let expected_frames = decode_frames(expected_body);
    let actual_frames = decode_frames(actual_body);

    if full_stream {
        assert_eq!(
            expected_frames.len(),
            actual_frames.len(),
            "Frame count didn't match.\n\
        Expected: {:?}\n\
        Actual:   {:?}",
            expected_frames,
            actual_frames
        );
    }

    for ((expected_wrapper, expected_message), (actual_wrapper, actual_message)) in
        expected_frames.into_iter().zip(actual_frames.into_iter())
    {
        assert_eq!(
            header_names(&expected_wrapper),
            header_names(&actual_wrapper)
        );
        if let Some(expected_message) = expected_message {
            let actual_message = actual_message.unwrap();
            assert_eq!(header_map(&expected_message), header_map(&actual_message));
            assert_eq!(expected_message.payload(), actual_message.payload());
        }
    }
    Ok(())
}

fn header_names(msg: &Message) -> BTreeSet<String> {
    msg.headers()
        .iter()
        .map(|h| h.name().as_str().into())
        .collect()
}
fn header_map(msg: &Message) -> BTreeMap<String, &HeaderValue> {
    msg.headers()
        .iter()
        .map(|h| (h.name().as_str().to_string(), h.value()))
        .collect()
}
