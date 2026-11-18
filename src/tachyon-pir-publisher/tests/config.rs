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
            "epoch_length 0 must be at least 2",
        ),
        (
            "epoch_length",
            json!(1),
            "epoch_length 1 must be at least 2",
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

#[test]
fn epoch_length_two_is_the_shortest_accepted() {
    let mut value = base();
    value["chain"]["epoch_length"] = json!(2);
    assert_eq!(
        parse(&value)
            .unwrap()
            .plan()
            .unwrap()
            .chain
            .unwrap()
            .epoch_length,
        2
    );
}

#[cfg(all(feature = "memo", feature = "first-contact", feature = "tachygram"))]
#[test]
fn the_example_configuration_is_valid() {
    let example: Value = serde_json::from_str(include_str!("../publisher.example.json")).unwrap();
    let plan = parse(&example).unwrap().plan().unwrap();
    assert_eq!(plan.product_names(), ["memo", "tachygram"]);
    assert_eq!(plan.publishers[0].resources.probes.len(), 2);
    assert_eq!(plan.chain.unwrap().epoch_length, 4096);
}

#[cfg(feature = "memo")]
mod memo {
    use super::*;

    fn settings() -> Value {
        json!({"listen": "127.0.0.1:9401", "state_dir": "/var/lib/tachyon-pir/memo"})
    }

    fn with_memo(first_contact: Option<Value>) -> Value {
        let mut value = base();
        value["memo"] = json!({"enabled": true, "settings": settings()});
        if let Some(first_contact) = first_contact {
            value["first_contact"] = first_contact;
        }
        value
    }

    #[test]
    fn memo_alone_registers_one_chain_following_definition() {
        let plan = parse(&with_memo(None)).unwrap().plan().unwrap();
        assert_eq!(plan.product_names(), ["memo"]);
        let definition = &plan.publishers[0];
        assert!(definition.follows_chain());
        assert_eq!(
            definition.resources.listeners,
            ["127.0.0.1:9401".parse().unwrap()]
        );
        assert!(
            definition
                .resources
                .writable_paths
                .contains(&"/var/lib/tachyon-pir/memo".into())
        );
        let probes: Vec<_> = definition.resources.probes.iter().map(|p| p.path).collect();
        assert_eq!(probes, ["/provider/memo/v1/current"]);
    }

    #[test]
    fn memo_settings_are_checked() {
        let mut value = with_memo(None);
        value["memo"]["settings"]["difficulty_bits"] = json!(20);
        let error = refusal(value);
        assert!(
            error.contains("memo.settings: unknown field `difficulty_bits`"),
            "{error}"
        );

        let mut value = with_memo(None);
        value["memo"]["settings"]["state_dir"] = json!("memo");
        assert!(refusal(value).contains("must be absolute"));

        let mut value = with_memo(None);
        value["memo"]["settings"]["listen"] = json!("0.0.0.0:9401");
        assert!(refusal(value).contains("fixed port"));

        let mut value = base();
        value["memo"] = json!({"enabled": true});
        assert!(refusal(value).contains("memo is enabled but has no settings"));

        let mut value = with_memo(None);
        value.as_object_mut().unwrap().remove("chain");
        assert!(refusal(value).contains("memo is enabled but there is no chain section"));

        // A disabled section registers nothing, but its settings are checked.
        let mut value = base();
        value["memo"] = json!({"enabled": false});
        assert!(parse(&value).unwrap().plan().unwrap().publishers.is_empty());
        value["memo"]["settings"] = json!({"listen": "127.0.0.1:9401"});
        assert!(refusal(value).contains("memo.settings: missing field `state_dir`"));
    }

    #[cfg(feature = "first-contact")]
    #[test]
    fn first_contact_joins_the_memo_definition() {
        let value = with_memo(Some(json!({"enabled": true, "settings": {}})));
        let plan = parse(&value).unwrap().plan().unwrap();
        assert_eq!(plan.product_names(), ["memo"]);
        let definition = &plan.publishers[0];
        assert_eq!(definition.resources.listeners.len(), 1);
        let probes: Vec<_> = definition.resources.probes.iter().map(|p| p.path).collect();
        assert_eq!(
            probes,
            [
                "/provider/memo/v1/current",
                "/provider/first-contact/v1/current"
            ]
        );

        // Disabled first-contact: memo alone, settings optional.
        for first_contact in [
            json!({"enabled": false}),
            json!({"enabled": false, "settings": {}}),
        ] {
            let plan = parse(&with_memo(Some(first_contact)))
                .unwrap()
                .plan()
                .unwrap();
            assert_eq!(plan.publishers[0].resources.probes.len(), 1);
        }
    }

    #[cfg(feature = "first-contact")]
    #[test]
    fn first_contact_settings_are_checked() {
        for settings in [
            json!({"listen": "127.0.0.1:9404"}),
            json!({"state_dir": "/x"}),
        ] {
            let value = with_memo(Some(json!({"enabled": true, "settings": settings})));
            let error = refusal(value);
            assert!(
                error.contains("first_contact.settings: unknown field"),
                "{error}"
            );
        }
        let value = with_memo(Some(json!({"enabled": true})));
        assert!(refusal(value).contains("first_contact is enabled but has no settings"));
        // Still refused without memo enabled, whatever its settings.
        let mut value = with_memo(Some(json!({"enabled": true, "settings": {}})));
        value["memo"]["enabled"] = json!(false);
        assert!(refusal(value).contains("first_contact is enabled but memo is not"));
    }

    #[cfg(feature = "tachygram")]
    #[test]
    fn memo_and_tachygram_need_disjoint_listeners_and_state() {
        let mut value = with_memo(None);
        value["tachygram"] = json!({"enabled": true, "settings": {
            "listen": "127.0.0.1:9402", "state_dir": "/var/lib/tachyon-pir/tachygram"}});
        let plan = parse(&value).unwrap().plan().unwrap();
        assert_eq!(plan.product_names(), ["memo", "tachygram"]);

        let mut shared_listener = value.clone();
        shared_listener["tachygram"]["settings"]["listen"] = json!("127.0.0.1:9401");
        let error = refusal(shared_listener);
        assert!(
            error.contains("conflicts with the memo listener"),
            "{error}"
        );

        let mut nested = value.clone();
        nested["tachygram"]["settings"]["state_dir"] = json!("/var/lib/tachyon-pir/memo/tachygram");
        let error = refusal(nested);
        assert!(
            error.contains("overlaps the memo state directory"),
            "{error}"
        );
    }
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
