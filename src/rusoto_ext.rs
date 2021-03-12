use async_trait::async_trait;
use rusoto_core::credential::{AwsCredentials, CredentialsError, ProvideAwsCredentials};

/// Rusoto generates client constructors that look like this:
///
/// ```skip
///  pub fn new_with<P, D>(
///      request_dispatcher: D,
///      credentials_provider: P,
///      region: region::Region,
///  ) -> QldbSessionClient
///  where
///      P: ProvideAwsCredentials + Send + Sync + 'static,
///      D: DispatchSignedRequest + Send + Sync + 'static,
///  {
/// ```
///
/// While the constructor is generic, the concrete type is not. This is nice,
/// because the user-exposed type isn't generic over the specific credential
/// provider, etc.
///
/// The [`QldbDriverBuilder`] has a similar design challenge, but the fluent
/// builder design doesn't leave the option actually accepting generics. So, we
/// use a Box type in our builder, then implement `ProvideAwsCredentials` for
/// that type. This satisfies the above constraints whilst keeping our API
/// clean.
pub(crate) fn into_boxed<P>(unboxed: P) -> BoxedCredentialsProvider
where
    P: ProvideAwsCredentials + Send + Sync + 'static,
{
    BoxedCredentialsProvider {
        inner: Box::new(unboxed),
    }
}

pub(crate) struct BoxedCredentialsProvider {
    inner: Box<dyn ProvideAwsCredentials + Send + Sync>,
}

#[async_trait]
impl ProvideAwsCredentials for BoxedCredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        self.inner.credentials().await
    }
}
