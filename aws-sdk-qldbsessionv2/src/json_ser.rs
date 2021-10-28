// Code generated by software.amazon.smithy.rust.codegen.smithy-rs. DO NOT EDIT.
pub fn serialize_structure_crate_model_start_transaction_request(
    object: &mut aws_smithy_json::serialize::JsonObjectWriter,
    input: &crate::model::StartTransactionRequest,
) {
    let (_, _) = (object, input);
}

pub fn serialize_structure_crate_model_end_session_request(
    object: &mut aws_smithy_json::serialize::JsonObjectWriter,
    input: &crate::model::EndSessionRequest,
) {
    let (_, _) = (object, input);
}

pub fn serialize_structure_crate_model_commit_transaction_request(
    object: &mut aws_smithy_json::serialize::JsonObjectWriter,
    input: &crate::model::CommitTransactionRequest,
) {
    if let Some(var_1) = &input.transaction_id {
        object.key("TransactionId").string(var_1);
    }
}

pub fn serialize_structure_crate_model_abort_transaction_request(
    object: &mut aws_smithy_json::serialize::JsonObjectWriter,
    input: &crate::model::AbortTransactionRequest,
) {
    let (_, _) = (object, input);
}

pub fn serialize_structure_crate_model_execute_statement_request(
    object: &mut aws_smithy_json::serialize::JsonObjectWriter,
    input: &crate::model::ExecuteStatementRequest,
) {
    if let Some(var_2) = &input.transaction_id {
        object.key("TransactionId").string(var_2);
    }
    if let Some(var_3) = &input.statement {
        object.key("Statement").string(var_3);
    }
    if let Some(var_4) = &input.parameters {
        let mut array_5 = object.key("Parameters").start_array();
        for item_6 in var_4 {
            {
                let mut object_7 = array_5.value().start_object();
                crate::json_ser::serialize_structure_crate_model_value_holder(
                    &mut object_7,
                    item_6,
                );
                object_7.finish();
            }
        }
        array_5.finish();
    }
}

pub fn serialize_structure_crate_model_fetch_page_request(
    object: &mut aws_smithy_json::serialize::JsonObjectWriter,
    input: &crate::model::FetchPageRequest,
) {
    if let Some(var_8) = &input.transaction_id {
        object.key("TransactionId").string(var_8);
    }
    if let Some(var_9) = &input.next_page_token {
        object.key("NextPageToken").string(var_9);
    }
}

pub fn serialize_structure_crate_model_value_holder(
    object: &mut aws_smithy_json::serialize::JsonObjectWriter,
    input: &crate::model::ValueHolder,
) {
    if let Some(var_10) = &input.ion_binary {
        object
            .key("IonBinary")
            .string_unchecked(&aws_smithy_types::base64::encode(var_10));
    }
    if let Some(var_11) = &input.ion_text {
        object.key("IonText").string(var_11);
    }
}