# Amazon QLDB Rust Driver

This is the Rust driver for Amazon Quantum Ledger Database (QLDB), which allows Rust developers
to write software that makes use of Amazon QLDB.

**This is a preview of the Amazon QLDB Driver for Rust, and we do not recommend that it be used for production purposes.**

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
