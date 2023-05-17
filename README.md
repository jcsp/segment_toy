# Redpanda Engineering Storage Tool

## Purpose

This low level tool is for offline use by Redpanda Engineering.

This tool is **not** for everyday use on live clusters.

## Quickstart

    # Get a rust toolchain
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

    # Compile and run
    cargo run --bin=rp-storage-tool --release -- --backend=<aws|gcp|azure> scan --source=<bucket name>

## Usage

### All commands

* The `--backend` argument selects the cloud storage backend. This has a default (AWS) for convenience when working with
  commands that don't use cloud storage, but ordinarily you should be specifying it.
* If you are running on a node with authentication already set up (e.g. IAM Roles on AWS), this will Just Work.
  Otherwise you may need to set the appropriate access key/secret environment variables for the cloud platform you are
  connecting to.

### `scan`

This walks the objects in a bucket used by Redpanda Tiered Storage and reports on any inconsistencies.
**Not all issues this tool reports are harmful**, for example `segments_outside_manifest` may contain objects harmlessly
left behind when Redpanda was restarted during an upload.

## Working with local object stores

This tool uses environment variables for any special configuration of storage backends required for working outside of
real cloud environments.

For example, to use the tool with a bucket called `data` in a minio cluster at `aplite:9000`:

    AWS_ALLOW_HTTP=1 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
      AWS_ENDPOINT=http://aplite:9000 cargo run --bin=rp-storage-tool --release -- --backend aws scan --source data