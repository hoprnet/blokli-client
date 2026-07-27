# Blokli Inspector

`blokli-inspector` is a command-line tool for inspecting a running [Blokli](https://hoprnet.org/) instance through its GraphQL API and
transaction endpoints. It is a thin CLI wrapper around the [`blokli-client`](https://crates.io/crates/blokli-client) library.

## Installation

```bash
cargo install blokli-inspector
```

## Usage

Every invocation needs the URL of the Blokli instance, either through `--url` or the `BLOKLI_URL` environment variable:

```bash
blokli-inspector --url https://blokli.example.org <COMMAND>
```

Output can be rendered as JSON (default), YAML, or human-readable tables via `--format`:

```bash
blokli-inspector --url https://blokli.example.org --format table channels
```

### Commands

- `query` — perform a one-shot query (accounts, channels, tickets, balances, safes, graph, chain info, …)
- `subscribe` — stream events from Blokli over SSE (accounts, channels, tickets, blocks, …)
- `transaction` — submit a hex-encoded signed transaction, optionally waiting for confirmations or tracking its status

Use `--help` on the binary or on any subcommand for the full list of selectors and filters:

```bash
blokli-inspector --help
blokli-inspector --url https://blokli.example.org query --help
```

## License

GPL-3.0-only
