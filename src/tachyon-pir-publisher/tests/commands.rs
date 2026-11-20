//! The command line of the built binary: `--check`, `--contract-info` and
//! usage errors. Nothing here contacts a node.
mod support;

use serde_json::json;
use std::ffi::OsStr;
use support::{free, run_to_completion, write_config};

fn check(config: &std::path::Path) -> std::process::Output {
    run_to_completion(&[
        OsStr::new("--config"),
        config.as_os_str(),
        OsStr::new("--check"),
    ])
}

/// The chain section names an address nothing listens on, so a `--check`
/// that contacted the node would fail.
fn valid() -> serde_json::Value {
    json!({
        "management_listen": free().to_string(),
        "chain": {"rpc_url": format!("http://{}", free()), "no_auth": true, "poll_ms": 100, "epoch_length": 16}
    })
}

#[test]
fn check_accepts_a_valid_file_offline_and_creates_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = valid();
    let mut products = Vec::new();
    if cfg!(feature = "memo") {
        config["memo"] = json!({"enabled": true, "settings": {
            "listen": free().to_string(),
            "state_dir": temp.path().join("memo"),
        }});
        products.push("memo");
    }
    if cfg!(feature = "first-contact") {
        config["first_contact"] = json!({"enabled": true, "settings": {}});
    }
    if cfg!(feature = "tachygram") {
        config["tachygram"] = json!({"enabled": true, "settings": {
            "listen": free().to_string(),
            "state_dir": temp.path().join("tachygram"),
        }});
        products.push("tachygram");
    }
    if cfg!(feature = "registry") {
        config["registry"] = json!({"enabled": true, "settings": {
            "listen": free().to_string(),
            "state_dir": temp.path().join("registry"),
            "difficulty_bits": 4,
        }});
        products.push("registry");
    }
    let path = write_config(temp.path(), "publisher.json", &config);
    let output = check(&path);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("is valid"), "{stdout}");
    assert!(stdout.contains("the node was not contacted"), "{stdout}");
    assert!(
        stdout.contains(&format!("products [{}]", products.join(", "))),
        "{stdout}"
    );
    // No state directory, lock or log was created.
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[test]
fn check_rejects_an_unknown_field_with_status_one() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = valid();
    config["chain"]["rpc_user"] = json!("zakura");
    let path = write_config(temp.path(), "publisher.json", &config);
    let output = check(&path);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("unknown field `rpc_user`"), "{stderr}");
    assert!(
        stderr.contains(&path.display().to_string()),
        "the error names the file: {stderr}"
    );
    assert!(output.stdout.is_empty());
}

#[test]
fn check_rejects_a_one_block_epoch() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = valid();
    config["chain"]["epoch_length"] = json!(1);
    let path = write_config(temp.path(), "publisher.json", &config);
    let output = check(&path);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("epoch_length 1 must be at least 2"),
        "{stderr}"
    );
}

/// A build without every product refuses a disabled section of one it lacks.
#[cfg(not(all(
    feature = "memo",
    feature = "first-contact",
    feature = "tachygram",
    feature = "registry"
)))]
#[test]
fn check_rejects_a_section_for_a_product_not_compiled_in() {
    use tachyon_pir_publisher::Product;
    let product = Product::ALL
        .into_iter()
        .find(|p| !p.compiled())
        .expect("a product this build lacks");
    let temp = tempfile::tempdir().unwrap();
    let mut config = valid();
    config[product.section()] = json!({"enabled": false});
    let path = write_config(temp.path(), "publisher.json", &config);
    let output = check(&path);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains(&format!("`{}` section", product.section()))
            && stderr.contains(&format!("the `{}` feature", product.feature())),
        "{stderr}"
    );
}

/// `--check` validates the registry's settings offline.
#[cfg(feature = "registry")]
#[test]
fn check_rejects_bad_registry_settings_offline() {
    let temp = tempfile::tempdir().unwrap();
    for (settings, expected) in [
        (
            json!({"listen": free().to_string(), "state_dir": temp.path().join("r"), "difficulty_bits": 257}),
            "registry difficulty_bits 257 is above 256",
        ),
        (
            json!({"listen": free().to_string(), "state_dir": temp.path().join("r"), "pow_bits": 4}),
            "registry.settings: unknown field `pow_bits`",
        ),
    ] {
        let mut config = valid();
        config["registry"] = json!({"enabled": true, "settings": settings});
        let path = write_config(temp.path(), "publisher.json", &config);
        let output = check(&path);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{stderr}");
        assert!(stderr.contains(expected), "{stderr}");
    }
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[test]
fn check_rejects_an_unreadable_file_and_malformed_json() {
    let temp = tempfile::tempdir().unwrap();
    let output = check(&temp.path().join("absent.json"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("cannot read configuration"), "{stderr}");

    let path = temp.path().join("broken.json");
    std::fs::write(&path, b"{\"management_listen\": ").unwrap();
    let output = check(&path);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("EOF while parsing"), "{stderr}");
}

#[test]
fn usage_errors_exit_with_status_two() {
    for args in [
        &["--check"][..],
        &[][..],
        &["--contract-info", "--check"][..],
    ] {
        let args: Vec<&OsStr> = args.iter().map(OsStr::new).collect();
        let output = run_to_completion(&args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn contract_info_lists_the_compiled_products() {
    let temp = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(support::BINARY)
        .current_dir(temp.path())
        .arg("--contract-info")
        .output()
        .unwrap();
    assert!(output.status.success());
    let info: pir_service_discovery::ContractInfo = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        info,
        pir_service_discovery::ContractInfo::new(
            "tachyon-pir-publisher",
            tachyon_pir_publisher::PRODUCTS,
            &[]
        )
    );
    assert_eq!(
        info.products.iter().any(|p| p == "tachygram"),
        cfg!(feature = "tachygram")
    );
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}
