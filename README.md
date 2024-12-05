# DeLorean

DeLorean is a time machine that traverses the history of a git repository. For each change to a JSON file, it records what exactly changed (added, removed, or modified) and when it happened. It is used as a one-time utility for analyzing the history of CourseTable course data.

## Usage

First make sure you have the Rust toolchain installed; check the [Rust book](https://doc.rust-lang.org/book/ch01-01-installation.html) for instructions.

```bash
cargo build
cargo run -- --help
```

## Options

Check `cargo run -- --help` for the most up-to-date options.

Here's how we run it for CourseTable ferry-data:

```sh
cargo run -- ../ferry-data output --primary-key crn --include parsed_courses/*.json -a course-table@users.noreply.github.com -a coursetable.at.yale@gmail.com -a git@harshal.sheth.io -a github-bot@harshal.sheth.io -a hsheth2@gmail.com --graveyard graveyard
```
