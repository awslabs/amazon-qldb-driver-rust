// Code generated by software.amazon.smithy.rust.codegen.smithy-rs. DO NOT EDIT.
#[derive(std::default::Default, std::clone::Clone, std::fmt::Debug)]
pub struct SendCommand {
    _private: (),
}
impl SendCommand {
    /// Creates a new builder-style object to manufacture [`SendCommandInput`](crate::input::SendCommandInput)
    pub fn builder() -> crate::input::send_command_input::Builder {
        crate::input::send_command_input::Builder::default()
    }
    pub fn new() -> Self {
        Self { _private: () }
    }
}
impl smithy_http::response::ParseHttpResponse for SendCommand {
    type Output =
        std::result::Result<crate::output::SendCommandOutput, crate::error::SendCommandError>;
    fn parse_unloaded(
        &self,
        response: &mut smithy_http::operation::Response,
    ) -> Option<Self::Output> {
        // This is an error, defer to the non-streaming parser
        if !response.http().status().is_success() && response.http().status().as_u16() != 200 {
            return None;
        }
        Some(crate::operation_deser::parse_send_command(response))
    }
    fn parse_loaded(&self, response: &http::Response<bytes::Bytes>) -> Self::Output {
        // if streaming, we only hit this case if its an error
        crate::operation_deser::parse_send_command_error(response)
    }
}
