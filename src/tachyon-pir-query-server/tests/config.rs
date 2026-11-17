//! Configuration rules (spec 4.2, 11.7): unknown fields, products not
//! compiled in, the `combined` role and the offline checks.

use serde_json::{Value, json};
use tachyon_pir_query_server::Config;

fn settings() -> Value {
    json!({
        "publisher": "https://publisher.example/",
        "state_dir": "/var/lib/tachyon-pir-query/tachygram",
        "publisher_ca": "/etc/tachyon-pir-query/publisher-ca.pem",
        "role": "combined",
        "publisher_network": "test",
        "publisher_genesis": "ab".repeat(32),
        "activation_height": 8,
        "epoch_length": 4096,
    })
}

fn config(section: &str, settings: Value) -> Value {
    json!({
        "management_listen": "127.0.0.1:9500",
        "public_listen": "127.0.0.1:9510",
        section: {"enabled": true, "settings": settings},
    })
}

fn validate(value: &Value) -> Result<tachyon_pir_query_server::Validated, String> {
    Config::parse(value.to_string().as_bytes())
        .and_then(|config| config.validate())
        .map_err(|error| error.to_string())
}

#[test]
fn unknown_fields_are_refused_everywhere() {
    let mut top = config("tachygram", settings());
    top["typo"] = true.into();
    assert!(Config::parse(top.to_string().as_bytes()).is_err());
    let mut inner = settings();
    inner["poll_seconds"] = 5.into();
    assert!(Config::parse(config("tachygram", inner).to_string().as_bytes()).is_err());
    let mut admission = settings();
    admission["admission"] = json!({"executing": 2, "queue": 8});
    assert!(Config::parse(config("tachygram", admission).to_string().as_bytes()).is_err());
    let mut section = config("tachygram", settings());
    section["tachygram"]["extra"] = 1.into();
    assert!(Config::parse(section.to_string().as_bytes()).is_err());
}

fn compiled(section: &str) -> bool {
    (section == "memo" && cfg!(feature = "memo"))
        || (section == "first_contact" && cfg!(feature = "first-contact"))
        || (section == "tachygram" && cfg!(feature = "tachygram"))
}

#[test]
fn a_section_for_a_product_not_compiled_in_is_refused() {
    // The registry has no serving module in this milestone.
    for section in ["memo", "first_contact", "tachygram", "registry"] {
        let mut s = settings();
        s["state_dir"] = format!("/var/lib/tachyon-pir-query/{section}").into();
        let enabled = validate(&config(section, s.clone()));
        let mut disabled = config(section, s);
        disabled[section]["enabled"] = false.into();
        if compiled(section) {
            let validated = enabled.unwrap();
            assert_eq!(validated.products[0].module.section, section);
        } else {
            let error = enabled.unwrap_err();
            assert!(error.contains("not compiled"), "{section}: {error}");
            let error = validate(&disabled).unwrap_err();
            assert!(
                error.contains("not compiled"),
                "disabled {section}: {error}"
            );
        }
    }
}

#[cfg(all(feature = "memo", feature = "first-contact"))]
#[test]
fn memo_and_first_contact_are_separate_products() {
    let mut value = config("memo", settings());
    let mut fc = settings();
    fc["state_dir"] = "/var/lib/tachyon-pir-query/first-contact".into();
    value["first_contact"] = json!({"enabled": true, "settings": fc});
    let validated = validate(&value).unwrap();
    let sections: Vec<_> = validated
        .products
        .iter()
        .map(|p| p.module.section)
        .collect();
    assert_eq!(sections, ["memo", "first_contact"]);
    assert_eq!(validated.products[0].retain_scopes, Some(64));
    assert_eq!(
        validated.products[1].retain_scopes, None,
        "first-contact keeps every span"
    );
    // One state directory cannot serve two products.
    value["first_contact"]["settings"]["state_dir"] =
        "/var/lib/tachyon-pir-query/tachygram/fc".into();
    assert!(validate(&value).unwrap_err().contains("overlapping"));
}

#[cfg(feature = "tachygram")]
#[test]
fn a_valid_configuration_takes_the_product_defaults() {
    let validated = validate(&config("tachygram", settings())).unwrap();
    let [product] = validated.products.as_slice() else {
        panic!("one product")
    };
    assert_eq!(product.module.section, "tachygram");
    assert_eq!(product.retain_scopes, Some(64));
    assert_eq!(product.max_unconfirmed.as_secs(), 600);
    assert_eq!(product.admission.executing, 2);
    assert_eq!(product.admission.waiting, 8);
    assert_eq!(product.admission.wait_ms, 2000);
    assert_eq!(product.admission.per_client, 2);
    assert_eq!(product.admission.upload_ms, 15_000);
    assert_eq!(product.network, "test");
    let mut tuned = settings();
    tuned["retain_scopes"] = 3.into();
    tuned["max_unconfirmed_seconds"] = 30.into();
    tuned["admission"] = json!({"executing": 4});
    let validated = validate(&config("tachygram", tuned)).unwrap();
    assert_eq!(validated.products[0].retain_scopes, Some(3));
    assert_eq!(validated.products[0].max_unconfirmed.as_secs(), 30);
    assert_eq!(validated.products[0].admission.executing, 4);
    assert_eq!(validated.products[0].admission.waiting, 8);
}

#[cfg(feature = "tachygram")]
type Mutation = Box<dyn Fn(&mut Value)>;

#[cfg(feature = "tachygram")]
#[test]
fn invalid_settings_are_refused() {
    let cases: Vec<(&str, Mutation)> = vec![
        ("role", Box::new(|s| s["role"] = "evaluator".into())),
        (
            "publisher scheme",
            Box::new(|s| s["publisher"] = "http://publisher.example/".into()),
        ),
        (
            "relative state",
            Box::new(|s| s["state_dir"] = "state".into()),
        ),
        (
            "relative ca",
            Box::new(|s| s["publisher_ca"] = "ca.pem".into()),
        ),
        (
            "genesis",
            Box::new(|s| s["publisher_genesis"] = "AB".repeat(32).into()),
        ),
        (
            "no activation",
            Box::new(|s| {
                s.as_object_mut().unwrap().remove("activation_height");
            }),
        ),
        ("zero epoch", Box::new(|s| s["epoch_length"] = 0.into())),
        ("retain zero", Box::new(|s| s["retain_scopes"] = 0.into())),
        (
            "unconfirmed",
            Box::new(|s| s["max_unconfirmed_seconds"] = 0.into()),
        ),
        (
            "executing",
            Box::new(|s| s["admission"] = json!({"executing": 0})),
        ),
        (
            "upload",
            Box::new(|s| s["admission"] = json!({"upload_ms": 0})),
        ),
    ];
    for (name, mutate) in cases {
        let mut s = settings();
        mutate(&mut s);
        assert!(validate(&config("tachygram", s)).is_err(), "{name}");
    }
    let mut public = config("tachygram", settings());
    public["management_listen"] = "10.0.0.1:9500".into();
    assert!(validate(&public).is_err(), "management must be loopback");
    let mut port = config("tachygram", settings());
    port["public_listen"] = "127.0.0.1:0".into();
    assert!(validate(&port).is_err(), "fixed ports");
    let mut same = config("tachygram", settings());
    same["public_listen"] = "127.0.0.1:9500".into();
    assert!(validate(&same).is_err(), "distinct listeners");
    let mut none = config("tachygram", settings());
    none["tachygram"]["enabled"] = false.into();
    assert!(validate(&none).unwrap_err().contains("no compiled product"));
    let mut missing = config("tachygram", settings());
    missing["tachygram"]
        .as_object_mut()
        .unwrap()
        .remove("settings");
    assert!(validate(&missing).is_err(), "enabled without settings");
}
