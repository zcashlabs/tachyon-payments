//! The configuration schema and every static rule (spec 4.2, 11.7), through
//! the library the binary uses.
use serde_json::{Value, json};
use tachyon_pir_publisher::{Config, PRODUCTS, Product};

fn parse(value: &Value) -> Result<Config, String> {
    Config::from_json(&serde_json::to_vec(value).unwrap()).map_err(|e| e.to_string())
}

/// The error of parsing and planning `value`.
fn refusal(value: Value) -> String {
    match parse(&value) {
        Err(error) => error,
        Ok(config) => match config.plan() {
            Err(error) => error.to_string(),
            Ok(plan) => panic!("accepted {value}: products {:?}", plan.product_names()),
        },
    }
}

fn chain() -> Value {
    json!({"rpc_url": "http://127.0.0.1:18232", "no_auth": true, "poll_ms": 100, "epoch_length": 16})
}

fn base() -> Value {
    json!({"management_listen": "127.0.0.1:9400", "chain": chain()})
}

#[test]
fn management_and_chain_alone_plan_no_products() {
    let config = parse(&base()).unwrap();
    let plan = config.plan().unwrap();
    assert!(plan.publishers.is_empty());
    assert_eq!(plan.management_listen.to_string(), "127.0.0.1:9400");
    let chain = plan.chain.unwrap();
    assert_eq!(chain.epoch_length, 16);
    assert_eq!(chain.settings.rpc_url, "http://127.0.0.1:18232");
    assert_eq!(chain.settings.rpc_cookie, None);
    assert_eq!(chain.settings.poll, std::time::Duration::from_millis(100));

    let management_only = parse(&json!({"management_listen": "127.0.0.1:9400"})).unwrap();
    let plan = management_only.plan().unwrap();
    assert!(plan.chain.is_none() && plan.publishers.is_empty());
}

#[test]
fn poll_ms_defaults_to_one_second_and_cookie_auth_is_accepted() {
    let mut value = base();
    value["chain"] = json!({"rpc_url": "https://node.example:18232", "rpc_cookie": "/var/lib/zakura/.cookie",
                            "epoch_length": 4096});
    let plan = parse(&value).unwrap().plan().unwrap();
    let chain = plan.chain.unwrap();
    assert_eq!(chain.settings.poll, std::time::Duration::from_secs(1));
    assert_eq!(
        chain.settings.rpc_cookie.as_deref(),
        Some(std::path::Path::new("/var/lib/zakura/.cookie"))
    );
}

#[test]
fn unknown_fields_are_refused_at_every_level() {
    let mut top = base();
    top["metrics_listen"] = json!("127.0.0.1:9409");
    assert!(refusal(top).contains("unknown field `metrics_listen`"));

    let mut in_chain = base();
    in_chain["chain"]["rpc_password"] = json!("secret");
    assert!(refusal(in_chain).contains("unknown field `rpc_password`"));

    for product in Product::ALL {
        let mut in_section = base();
        in_section[product.section()] = json!({"enabled": false, "listen": "127.0.0.1:9402"});
        let error = refusal(in_section);
        assert!(
            error.contains("unknown field `listen`"),
            "{}: {error}",
            product.section()
        );
    }
}

#[test]
fn sections_need_enabled() {
    let mut value = base();
    value["tachygram"] = json!({"settings": {}});
    assert!(refusal(value).contains("missing field `enabled`"));
}

#[test]
fn chain_needs_epoch_length_and_rpc_url() {
    let mut value = base();
    value["chain"]
        .as_object_mut()
        .unwrap()
        .remove("epoch_length");
    assert!(refusal(value).contains("missing field `epoch_length`"));
    let mut value = base();
    value["chain"].as_object_mut().unwrap().remove("rpc_url");
    assert!(refusal(value).contains("missing field `rpc_url`"));
}

/// Every section of a product this build lacks is refused, enabled or not,
/// and the error names the Cargo feature.
#[test]
fn sections_for_products_not_compiled_in_are_refused_naming_the_feature() {
    let mut refused = 0;
    for product in Product::ALL.into_iter().filter(|p| !p.compiled()) {
        for enabled in [false, true] {
            let mut value = base();
            value[product.section()] = json!({"enabled": enabled, "settings": {}});
            if product == Product::FirstContact && enabled {
                value["memo"] = json!({"enabled": true, "settings": {}});
            }
            let error = refusal(value);
            let section =
                if product == Product::FirstContact && enabled && !Product::Memo.compiled() {
                    // The memo section is checked first; it is also not compiled.
                    Product::Memo
                } else {
                    product
                };
            assert!(
                error.contains(&format!("`{}` section", section.section()))
                    && error.contains(&format!("the `{}` feature", section.feature())),
                "{}: {error}",
                product.section()
            );
            refused += 1;
        }
    }
    assert_eq!(refused, 2 * (4 - PRODUCTS.len()));
    assert_eq!(PRODUCTS.contains(&"tachygram"), cfg!(feature = "tachygram"));
}

#[test]
fn first_contact_requires_memo_enabled() {
    for memo in [None, Some(json!({"enabled": false}))] {
        let mut value = base();
        value["first_contact"] = json!({"enabled": true, "settings": {}});
        if let Some(memo) = memo {
            value["memo"] = memo;
        }
        let error = refusal(value);
        assert!(
            error.contains("first_contact is enabled but memo is not"),
            "{error}"
        );
    }
}

#[test]
fn management_listener_must_be_loopback_with_a_fixed_port() {
    for (address, expected) in [
        ("0.0.0.0:9400", "must be a loopback address"),
        ("10.1.2.3:9400", "must be a loopback address"),
        ("127.0.0.1:0", "fixed port"),
        ("localhost:9400", "invalid socket address"),
    ] {
        let mut value = base();
        value["management_listen"] = json!(address);
        let error = refusal(value);
        assert!(error.contains(expected), "{address}: {error}");
    }
}

#[test]
fn chain_rules() {
    for (field, replacement, expected) in [
        ("rpc_url", json!("ftp://node:18232"), "http(s) URL"),
        ("rpc_url", json!("not a url"), "chain rpc_url"),
        (
            "rpc_url",
            json!("http://user:pw@node:18232"),
            "must not carry credentials",
        ),
        (
            "rpc_cookie",
            json!("/var/lib/zakura/.cookie"),
            "both rpc_cookie and no_auth",
        ),
        ("no_auth", json!(false), "needs rpc_cookie"),
        ("poll_ms", json!(0), "poll_ms must be nonzero"),
        (
            "epoch_length",
            json!(0),
            "epoch_length must be at least one block",
        ),
        ("epoch_length", json!(-1), "invalid value"),
    ] {
        let mut value = base();
        value["chain"][field] = replacement.clone();
        let error = refusal(value);
        assert!(error.contains(expected), "{field}={replacement}: {error}");
    }
    let mut relative = base();
    relative["chain"] = json!({"rpc_url": "http://127.0.0.1:18232", "rpc_cookie": "zakura/.cookie", "epoch_length": 16});
    assert!(refusal(relative).contains("rpc_cookie zakura/.cookie must be absolute"));
}

#[cfg(feature = "tachygram")]
mod tachygram {
    use super::*;

    fn with_tachygram(settings: Value) -> Value {
        let mut value = base();
        value["tachygram"] = json!({"enabled": true, "settings": settings});
        value
    }

    fn settings() -> Value {
        json!({"listen": "127.0.0.1:9402", "state_dir": "/var/lib/tachyon-pir/tachygram"})
    }

    #[test]
    fn the_example_configuration_is_valid() {
        let example: Value =
            serde_json::from_str(include_str!("../publisher.example.json")).unwrap();
        let plan = parse(&example).unwrap().plan().unwrap();
        assert_eq!(plan.product_names(), ["tachygram"]);
        assert_eq!(plan.chain.unwrap().epoch_length, 4096);
    }

    #[test]
    fn an_enabled_tachygram_section_registers_one_chain_following_definition() {
        let plan = parse(&with_tachygram(settings())).unwrap().plan().unwrap();
        assert_eq!(plan.product_names(), ["tachygram"]);
        let definition = &plan.publishers[0];
        assert!(definition.follows_chain());
        assert_eq!(
            definition.resources.listeners,
            ["127.0.0.1:9402".parse().unwrap()]
        );
        assert!(
            definition
                .resources
                .writable_paths
                .contains(&"/var/lib/tachyon-pir/tachygram".into())
        );
    }

    #[test]
    fn a_disabled_section_registers_nothing_and_needs_no_settings() {
        let mut value = base();
        value["tachygram"] = json!({"enabled": false});
        assert!(parse(&value).unwrap().plan().unwrap().publishers.is_empty());
        // Settings that are present are still checked.
        value["tachygram"]["settings"] = json!({"listen": "127.0.0.1:9402"});
        assert!(refusal(value).contains("tachygram.settings: missing field `state_dir`"));
    }

    #[test]
    fn unknown_settings_fields_are_refused() {
        let mut settings = settings();
        settings["epoch_length"] = json!(16);
        let error = refusal(with_tachygram(settings));
        assert!(
            error.contains("tachygram.settings: unknown field `epoch_length`"),
            "{error}"
        );
    }

    #[test]
    fn enabled_needs_settings_and_the_chain() {
        let mut value = base();
        value["tachygram"] = json!({"enabled": true});
        assert!(refusal(value).contains("tachygram is enabled but has no settings"));

        let mut value = with_tachygram(settings());
        value.as_object_mut().unwrap().remove("chain");
        assert!(refusal(value).contains("tachygram is enabled but there is no chain section"));
    }

    #[test]
    fn a_relative_state_dir_is_refused() {
        let mut settings = settings();
        settings["state_dir"] = json!("state/tachygram");
        let error = refusal(with_tachygram(settings));
        assert!(error.contains("must be absolute"), "{error}");
    }

    #[test]
    fn bad_listeners_are_refused() {
        for (address, expected) in [
            ("8.8.8.8:9402", "fixed port"),
            ("0.0.0.0:9402", "fixed port"),
            ("127.0.0.1:0", "fixed port"),
            ("127.0.0.1:9400", "conflicts with the management listener"),
        ] {
            let mut settings = settings();
            settings["listen"] = json!(address);
            let error = refusal(with_tachygram(settings));
            assert!(error.contains(expected), "{address}: {error}");
        }
    }
}
