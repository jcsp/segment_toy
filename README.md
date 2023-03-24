# Engineering Storage Tool

## Purpose

This tool is for offline use by Redpanda Engineering.

## Quickstart

    # Get a rust toolchain
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

    # Compile and run
    cargo run --release -- --backend=<aws|gcp|azure> scan --source=<bucket name>

## Usage

### All commands

* The `--backend` argument is mandatory.
* If you are running on a node with authentication already set up (e.g. IAM Roles on AWS), this will Just Work.
  Otherwise you may need to set the appropriate access key/secret environment variables for the cloud platform you are
  connecting to.

### `scan`

This walks the objects in a bucket used by Redpanda Tiered Storage and reports on any inconsistencies.
**Not all issues this tool reports are harmful**, for example `segments_outside_manifest` may contain objects harmlessly
left behind when Redpanda was restarted during an upload.