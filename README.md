# Amazon QLDB Rust Driver

[![License](https://img.shields.io/hexpm/l/plug.svg)](https://github.com/awslabs/amazon-qldb-driver-rust/blob/main/LICENSE)
[![CI Build](https://github.com/awslabs/amazon-qldb-driver-rust/workflows/CI%20Build/badge.svg)](https://github.com/awslabs/amazon-qldb-driver-rust/actions?query=workflow%3A%22CI+Build%22)

This is the Rust driver for Amazon Quantum Ledger Database (QLDB), which allows Rust developers
to write software that makes use of Amazon QLDB.

**This package is considered experimental, under active/early development.**

## Requirements

### Basic Configuration

You need to set up your AWS security credentials and config before the driver is able to connect to AWS. 

Set up credentials (in e.g. `~/.aws/credentials`):

```
[default]
aws_access_key_id = <your access key id>
aws_secret_access_key = <your secret key>
```

Set up a default region (in e.g. `~/.aws/config`):

```
[default]
region = us-east-1 <or other region>
```

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html#SettingUp.Q.GetCredentials) page for more information.

### Rust

The driver is written in, and requires, Rust to build. Please see the link below to setup Rust on your system:

* [Rustup](https://rustup.rs/)

## Installing the Driver

To build the driver, run the following in the root directory:

```cargo build```

## Using the Driver as a Dependency

To use the driver, in your package that wishes to use the driver, run the following:

```cargo add amazon-qldb-driver```

## Development

### Running Tests

You can run the unit tests with this command:

```
$ cargo test
```

### Documentation 

RustDoc is used for documentation. You can generate HTML locally with the following:

```cargo doc```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

## Release Notes

### Release 0.1 (XXX, 2020)

Coming soon!
