use amazon_qldb_driver_core::{
    error::{QldbError, QldbResult},
    QldbDriver, QldbDriverBuilder,
};
use rusoto_core::{
    credential::{DefaultCredentialsProvider, ProvideAwsCredentials},
    Client, HttpClient, Region,
};
use rusoto_qldb_session::QldbSessionClient;

use crate::{
    convert::RusotoQldbSessionClient,
    rusoto_ext::{into_boxed, BoxedCredentialsProvider},
};

/// A builder to help you customize a [`QldbDriver`] backed by rusoto.
///
/// In many cases, it is sufficient to use [`QldbDriver::new`] to build a driver
/// out of a Rusoto client for a particular QLDB ledger. However, if you wish to
/// customize the driver beyond the defaults, this builder is what you want.
///
/// Note that the following setters _must_ be called, else [`build`] will return
/// an `Err`: - `ledger_name` - `client`
///
/// Usage example:
/// ```no_run
/// # use amazon_qldb_driver_core::QldbDriverBuilder;
/// # use amazon_qldb_driver_rusoto::QldbDriverBuilderExt;
/// # use rusoto_core::region::Region;
/// # use tokio;
/// #
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let driver = QldbDriverBuilder::new()
///     .ledger_name("sample-ledger")
///     .via_rusoto()
///     .region(Region::UsEast1)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait QldbDriverBuilderExt {
    fn via_rusoto(self) -> RusotoQldbDriverBuilder;
}

impl QldbDriverBuilderExt for QldbDriverBuilder {
    fn via_rusoto(self) -> RusotoQldbDriverBuilder {
        RusotoQldbDriverBuilder {
            builder: self,
            region: None,
            credentials_provider: None,
        }
    }
}

pub struct RusotoQldbDriverBuilder {
    builder: QldbDriverBuilder,
    region: Option<Region>,
    credentials_provider: Option<BoxedCredentialsProvider>,
}

impl RusotoQldbDriverBuilder {
    pub fn credentials_provider<P>(mut self, credentials_provider: P) -> Self
    where
        P: ProvideAwsCredentials + Send + Sync + 'static,
    {
        self.credentials_provider = Some(into_boxed(credentials_provider));
        self
    }

    pub fn region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }

    // FIXME: It's messy that we need to wrap credentials_provider in an Option
    // just so we can avoid the partial move of self here (into
    // build_with_client). Suggestion: refactor the builder to cleanly separate
    // out the client builder.
    pub async fn build(self) -> QldbResult<QldbDriver<RusotoQldbSessionClient>> {
        let credentials_provider = match self.credentials_provider {
            Some(c) => c,
            None => into_boxed(
                DefaultCredentialsProvider::new()
                    .map_err(|err| QldbError::IllegalState(format!("{}", err)))?,
            ),
        };
        let client = Client::new_with(credentials_provider, default_dispatcher()?);
        let region = match self.region {
            Some(r) => r,
            None => Region::default(),
        };
        let rusoto = QldbSessionClient::new_with_client(client, region);
        let wrapped = RusotoQldbSessionClient(rusoto);
        self.builder.build_with_client(wrapped).await
    }
}

fn default_dispatcher() -> QldbResult<HttpClient> {
    let mut client =
        HttpClient::new().map_err(|tls| QldbError::IllegalState(format!("{}", tls)))?;
    client.local_agent(format!(
        "QLDB Driver for Rust v{}",
        amazon_qldb_driver_core::version()
    ));
    Ok(client)
}
