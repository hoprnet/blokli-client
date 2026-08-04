use std::{env, fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use clap::{
    ArgAction, Parser,
    builder::{BoolishValueParser, TypedValueParser},
};
use hopr_types::chain::ContractAddresses;
use serde::Deserialize;
use url::Url;

const DEFAULT_INTEGRATION_CONFIG: &str = "config-integration-anvil.toml";
const DEFAULT_TEST_IMAGE: &str = "bloklid:integration-test";
const EXTERNAL_PORT_BASE_ENV: &str = "BLOKLI_TEST_PORT_BASE";
const EXTERNAL_RUN_ID_ENV: &str = "BLOKLI_TEST_RUN_ID";
/// Environment variable whose value must identify the workspace root used by integration tests.
const TEST_WORKSPACE_ROOT_ENV: &str = "BLOKLI_TEST_WORKSPACE_ROOT";
const STACK_PORT_STRIDE: u16 = 10;

/// Base ports for integration test stacks. Each stack offsets from these
/// using a deterministic value derived from the process ID.
const BASE_BLOKLID_PORT: u16 = 18081;
const BASE_ANVIL_PORT: u16 = 18546;
const BASE_REGISTRY_PORT: u16 = 15001;

/// Generates a short stack identifier from the process ID.
/// Each test binary runs as a separate process, so the PID gives natural uniqueness.
fn default_stack_id() -> String {
    format!("{:04x}", std::process::id() % 0xFFFF)
}

/// Computes a deterministic port offset (0..255) from a stack ID string.
fn port_offset(stack_id: &str) -> u16 {
    let hash: u16 = stack_id.bytes().fold(0u16, |acc, b| acc.wrapping_add(b as u16));
    hash % 256
}

#[derive(Debug, PartialEq)]
struct ExternalStackAssignment {
    stack_id: String,
    registry_port: u16,
    anvil_port: u16,
    bloklid_port: u16,
}

#[derive(Deserialize)]
struct IntegrationServiceConfig {
    /// Network name the pinned `contracts` addresses below should match in hopr-bindings'
    /// `contracts-addresses.json`. Only read by the `tests` module below (see
    /// `regenerate_contract_addresses_toml` and `config_contract_addresses_match_hopr_bindings`).
    #[allow(dead_code)]
    network: String,
    contracts: ContractAddresses,
}

#[derive(Parser, Debug, Clone)]
pub struct TestConfig {
    #[arg(skip = PathBuf::new())]
    pub project_root: PathBuf,

    #[arg(skip = PathBuf::new())]
    pub integration_dir: PathBuf,

    #[arg(long, env = "BLOKLI_TEST_IMAGE", default_value = DEFAULT_TEST_IMAGE)]
    pub bloklid_image: String,

    #[arg(long, env = "BLOKLI_TEST_CONFIG", default_value = DEFAULT_INTEGRATION_CONFIG)]
    pub integration_config: String,

    #[arg(long, env = "BLOKLI_TEST_REMOTE_IMAGE")]
    pub remote_image: Option<String>,

    #[arg(long, env = "BLOKLI_TEST_BLOKLID_URL")]
    pub bloklid_url: Option<Url>,

    #[arg(long, env = "BLOKLI_TEST_RPC_URL")]
    pub rpc_url: Option<Url>,

    #[arg(
        long,
        env = "BLOKLI_TEST_HTTP_TIMEOUT_SECS",
        default_value = "30",
        value_parser = clap::value_parser!(u64).map(Duration::from_secs)
    )]
    pub http_timeout: Duration,

    #[arg(long, env = "BLOKLI_TEST_CONFIRMATIONS", default_value_t = 1)]
    pub tx_confirmations: usize,

    #[arg(long, env = "BLOKLI_TEST_REGISTRY_PORT")]
    pub registry_port: Option<u16>,

    #[arg(long, env = "BLOKLI_TEST_STACK_ID", default_value_t = default_stack_id())]
    pub stack_id: String,

    /// Uses an externally managed Docker stack instead of managing one locally.
    #[arg(
        long,
        env = "BLOKLI_TEST_EXTERNAL_STACK",
        default_value_t = false,
        action = ArgAction::Set,
        value_parser = BoolishValueParser::new()
    )]
    pub external_stack: bool,
}

impl TestConfig {
    pub fn load() -> Result<Self> {
        let mut cfg = TestConfig::parse_from(["blokli-integration-config"]);
        cfg.finalize()?;
        Ok(cfg)
    }

    fn finalize(&mut self) -> Result<()> {
        let (project_root, integration_dir) = resolve_paths()?;
        self.project_root = project_root;
        self.integration_dir = integration_dir;

        if self.external_stack {
            self.configure_external_stack()?;
        } else {
            let offset = port_offset(&self.stack_id);

            if self.bloklid_url.is_none() {
                self.bloklid_url = Some(Url::parse(&format!("http://localhost:{}", self.bloklid_port(offset)))?);
            }
            if self.rpc_url.is_none() {
                self.rpc_url = Some(Url::parse(&format!("http://localhost:{}", self.anvil_port(offset)))?);
            }
            if self.registry_port.is_none() {
                self.registry_port = Some(BASE_REGISTRY_PORT + offset);
            }
        }

        Ok(())
    }

    fn configure_external_stack(&mut self) -> Result<()> {
        let current_exe = env::current_exe().context("Failed to resolve current integration test binary")?;
        let binary_name = current_exe
            .file_name()
            .and_then(|name| name.to_str())
            .context("Integration test binary name is not valid UTF-8")?;
        let run_id = env::var(EXTERNAL_RUN_ID_ENV)
            .with_context(|| format!("{EXTERNAL_RUN_ID_ENV} must be set for externally managed integration stacks"))?;
        let port_base = env::var(EXTERNAL_PORT_BASE_ENV)
            .with_context(|| format!("{EXTERNAL_PORT_BASE_ENV} must be set for externally managed integration stacks"))?
            .parse::<u16>()
            .with_context(|| format!("{EXTERNAL_PORT_BASE_ENV} must be a valid port number"))?;
        let assignment = external_stack_assignment(binary_name, &run_id, port_base)?;

        self.stack_id = assignment.stack_id;
        self.registry_port = Some(assignment.registry_port);
        self.rpc_url = Some(Url::parse(&format!("http://localhost:{}", assignment.anvil_port))?);
        self.bloklid_url = Some(Url::parse(&format!("http://localhost:{}", assignment.bloklid_port))?);
        Ok(())
    }

    pub fn bloklid_url(&self) -> &Url {
        self.bloklid_url.as_ref().expect("bloklid_url not initialized")
    }

    pub fn rpc_url(&self) -> &Url {
        self.rpc_url.as_ref().expect("rpc_url not initialized")
    }

    pub fn registry_port(&self) -> u16 {
        self.registry_port.expect("registry_port not initialized")
    }

    pub fn contract_addresses(&self) -> Result<ContractAddresses> {
        let config_path = self.integration_dir.join(&self.integration_config);
        let contents = fs::read_to_string(&config_path)
            .with_context(|| format!("Failed to read integration config at {}", config_path.display()))?;
        let config: IntegrationServiceConfig = toml_edit::de::from_str(&contents)
            .with_context(|| format!("Failed to parse integration config at {}", config_path.display()))?;
        Ok(config.contracts)
    }

    pub fn bloklid_port(&self, offset: u16) -> u16 {
        BASE_BLOKLID_PORT + offset
    }

    pub fn anvil_port(&self, offset: u16) -> u16 {
        BASE_ANVIL_PORT + offset
    }
}

fn external_stack_assignment(binary_name: &str, run_id: &str, port_base: u16) -> Result<ExternalStackAssignment> {
    let mut run_id_characters = run_id.chars();
    let has_valid_first_character = run_id_characters
        .next()
        .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit());
    ensure!(
        has_valid_first_character
            && run_id_characters
                .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'),
        "{EXTERNAL_RUN_ID_ENV} must start with a lowercase ASCII letter or digit and contain only lowercase ASCII \
         letters, digits, and hyphens"
    );

    let (stack_name, stack_index) = if binary_name.starts_with("blokli_query_client") {
        ("query", 0)
    } else if binary_name.starts_with("blokli_subscription_client") {
        ("subscription", 1)
    } else if binary_name.starts_with("blokli_transaction_client") {
        ("transaction", 2)
    } else if binary_name.starts_with("blokli_load") {
        ("load", 3)
    } else {
        bail!("Unsupported integration test binary for external Docker stack: {binary_name}");
    };

    let stack_offset = stack_index * STACK_PORT_STRIDE;
    let registry_port = port_base
        .checked_add(stack_offset)
        .context("Integration stack registry port exceeds u16 range")?;
    let anvil_port = registry_port
        .checked_add(1)
        .context("Integration stack Anvil port exceeds u16 range")?;
    let bloklid_port = registry_port
        .checked_add(2)
        .context("Integration stack bloklid port exceeds u16 range")?;

    Ok(ExternalStackAssignment {
        stack_id: format!("{run_id}-{stack_name}"),
        registry_port,
        anvil_port,
        bloklid_port,
    })
}

fn resolve_paths() -> Result<(PathBuf, PathBuf)> {
    if let Some(project_root) = env::var_os(TEST_WORKSPACE_ROOT_ENV) {
        return validate_workspace_root(PathBuf::from(project_root));
    }

    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let tests_dir = crate_dir
        .parent()
        .context("Failed to resolve tests directory")?
        .to_path_buf();
    let project_root = tests_dir
        .parent()
        .context("Failed to resolve workspace root")?
        .to_path_buf();
    validate_workspace_root(project_root)
}

fn validate_workspace_root(project_root: PathBuf) -> Result<(PathBuf, PathBuf)> {
    ensure!(
        !project_root.as_os_str().is_empty(),
        "{TEST_WORKSPACE_ROOT_ENV} must not be empty"
    );
    ensure!(
        project_root.join("Cargo.toml").is_file(),
        "{TEST_WORKSPACE_ROOT_ENV} must point to a workspace root containing Cargo.toml: {}",
        project_root.display()
    );

    let integration_dir = project_root.join("tests/integration");
    ensure!(
        integration_dir.is_dir(),
        "{TEST_WORKSPACE_ROOT_ENV} workspace root must contain tests/integration: {}",
        project_root.display()
    );

    Ok((project_root, integration_dir))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use clap::Parser;
    use tempfile::{TempDir, tempdir};

    use super::{
        DEFAULT_INTEGRATION_CONFIG, ExternalStackAssignment, IntegrationServiceConfig, TestConfig,
        external_stack_assignment, validate_workspace_root,
    };

    fn read_integration_service_config() -> IntegrationServiceConfig {
        let toml_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DEFAULT_INTEGRATION_CONFIG);
        let contents = fs::read_to_string(&toml_path).expect("config-integration-anvil.toml should be readable");
        toml_edit::de::from_str(&contents).expect("config-integration-anvil.toml should parse")
    }

    /// Guards against the addresses pinned in `config-integration-anvil.toml` silently drifting
    /// from hopr-bindings' `contracts-addresses.json` (e.g. after a `hopr-bindings` version bump
    /// changes the deployment order). Such drift makes `bloklid` watch the wrong addresses, so
    /// safe/channel/account indexing silently times out instead of failing fast.
    #[test]
    fn config_contract_addresses_match_hopr_bindings() {
        let config = read_integration_service_config();

        let (_, expected) = hopr_types::chain::contract_addresses_for_network(&config.network).unwrap_or_else(|| {
            panic!(
                "hopr-bindings contracts-addresses.json has no entry for network '{}'",
                config.network
            )
        });

        assert_eq!(
            config.contracts, expected,
            "{DEFAULT_INTEGRATION_CONFIG}'s [contracts] addresses have drifted from hopr-bindings' \
             contracts-addresses.json for network '{}'. Run `just regen-integration-contracts` to refresh them.",
            config.network
        );
    }

    /// Rewrites `config-integration-anvil.toml`'s `[contracts]` addresses from hopr-bindings'
    /// bundled `contracts-addresses.json`. Run via `just regen-integration-contracts` after
    /// bumping `hopr-bindings` (or whenever `config_contract_addresses_match_hopr_bindings` fails).
    #[test]
    #[ignore = "run explicitly via `just regen-integration-contracts`"]
    fn regenerate_contract_addresses_toml() {
        let config = read_integration_service_config();

        let (_, addresses) = hopr_types::chain::contract_addresses_for_network(&config.network).unwrap_or_else(|| {
            panic!(
                "hopr-bindings contracts-addresses.json has no entry for network '{}'",
                config.network
            )
        });

        let toml_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DEFAULT_INTEGRATION_CONFIG);
        let contents = fs::read_to_string(&toml_path).expect("config-integration-anvil.toml should be readable");
        let mut doc = contents
            .parse::<toml_edit::DocumentMut>()
            .expect("config-integration-anvil.toml should parse as a TOML document");
        let contracts = doc["contracts"]
            .as_table_mut()
            .expect("config-integration-anvil.toml should have a [contracts] table");
        contracts["announcements"] = toml_edit::value(addresses.announcements.to_string());
        contracts["channels"] = toml_edit::value(addresses.channels.to_string());
        contracts["module_implementation"] = toml_edit::value(addresses.module_implementation.to_string());
        contracts["node_safe_migration"] = toml_edit::value(addresses.node_safe_migration.to_string());
        contracts["node_safe_registry"] = toml_edit::value(addresses.node_safe_registry.to_string());
        contracts["node_stake_factory"] = toml_edit::value(addresses.node_stake_factory.to_string());
        contracts["ticket_price_oracle"] = toml_edit::value(addresses.ticket_price_oracle.to_string());
        contracts["token"] = toml_edit::value(addresses.token.to_string());
        contracts["winning_probability_oracle"] = toml_edit::value(addresses.winning_probability_oracle.to_string());
        contracts["xhopr_token"] = toml_edit::value(addresses.xhopr_token.to_string());

        fs::write(&toml_path, doc.to_string()).expect("config-integration-anvil.toml should be writable");
    }

    fn workspace_fixture(include_integration_dir: bool) -> TempDir {
        let workspace = tempdir().expect("temporary workspace should be created");
        fs::write(workspace.path().join("Cargo.toml"), "[workspace]\n").expect("workspace manifest should be created");
        if include_integration_dir {
            fs::create_dir_all(workspace.path().join("tests/integration"))
                .expect("integration directory should be created");
        }
        workspace
    }

    #[test]
    fn accepts_valid_workspace_root() {
        let workspace = workspace_fixture(true);
        let project_root = workspace.path().to_path_buf();

        let (resolved_root, integration_dir) =
            validate_workspace_root(project_root.clone()).expect("workspace root should be valid");

        assert_eq!(resolved_root, project_root);
        assert_eq!(integration_dir, resolved_root.join("tests/integration"));
    }

    #[test]
    fn rejects_invalid_workspace_roots() {
        let unrelated_workspace = workspace_fixture(false);
        let invalid_roots = [
            (PathBuf::new(), "must not be empty"),
            (
                unrelated_workspace.path().join("does-not-exist"),
                "containing Cargo.toml",
            ),
            (
                unrelated_workspace.path().to_path_buf(),
                "must contain tests/integration",
            ),
        ];

        for (project_root, expected_error) in invalid_roots {
            let error = validate_workspace_root(project_root).expect_err("invalid root must be rejected");

            assert!(error.to_string().contains(expected_error));
        }
    }

    #[test]
    fn assigns_distinct_external_stacks_per_test_binary() {
        let assignments = [
            (
                "blokli_query_client-hash",
                ExternalStackAssignment {
                    stack_id: "local-query".to_string(),
                    registry_port: 20_000,
                    anvil_port: 20_001,
                    bloklid_port: 20_002,
                },
            ),
            (
                "blokli_subscription_client-hash",
                ExternalStackAssignment {
                    stack_id: "local-subscription".to_string(),
                    registry_port: 20_010,
                    anvil_port: 20_011,
                    bloklid_port: 20_012,
                },
            ),
            (
                "blokli_transaction_client-hash",
                ExternalStackAssignment {
                    stack_id: "local-transaction".to_string(),
                    registry_port: 20_020,
                    anvil_port: 20_021,
                    bloklid_port: 20_022,
                },
            ),
            (
                "blokli_load-hash",
                ExternalStackAssignment {
                    stack_id: "local-load".to_string(),
                    registry_port: 20_030,
                    anvil_port: 20_031,
                    bloklid_port: 20_032,
                },
            ),
        ];

        for (binary_name, expected) in assignments {
            assert_eq!(
                external_stack_assignment(binary_name, "local", 20_000)
                    .expect("known integration binary should have a stack"),
                expected
            );
        }
    }

    #[test]
    fn rejects_invalid_external_stack_configuration() {
        assert!(external_stack_assignment("unknown-test", "local", 20_000).is_err());
        assert!(external_stack_assignment("blokli_load-hash", "INVALID", 20_000).is_err());
        assert!(external_stack_assignment("blokli_load-hash", "-local", 20_000).is_err());
        assert!(external_stack_assignment("blokli_load-hash", "local", u16::MAX).is_err());
    }

    #[test]
    fn parses_external_stack_boolean_values() {
        let disabled = TestConfig::try_parse_from(["test-config", "--external-stack", "false"])
            .expect("false should be accepted as an external stack value");
        let enabled = TestConfig::try_parse_from(["test-config", "--external-stack", "true"])
            .expect("true should be accepted as an external stack value");

        assert!(!disabled.external_stack);
        assert!(enabled.external_stack);
    }
}
