//! The resource rules `pir_publisher::run_registered` enforces at start-up
//! (`pir-publisher/src/config/validation.rs` and `layout.rs` at `40b0ba6a`),
//! applied here as well so that `--check` reports them offline. They are
//! private in `pir-publisher`, so they are restated, not reused; the host
//! checks them again before binding anything.
use crate::Result;
use pir_pipeline::publisher::PublisherDefinition;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Component, Path, PathBuf},
};

#[derive(Default)]
struct Layout {
    listeners: Vec<(SocketAddr, &'static str)>,
    writable_paths: Vec<(&'static str, PathBuf)>,
    names: Vec<&'static str>,
}

/// Check the management listener and every definition's declared resources.
pub(crate) fn validate(
    management_listen: SocketAddr,
    publishers: &[PublisherDefinition<axum::Router>],
) -> Result<()> {
    if !management_listen.ip().is_loopback() {
        return Err(
            format!("management_listen {management_listen} must be a loopback address").into(),
        );
    }
    let mut layout = Layout::default();
    layout.listener("management", management_listen)?;
    for publisher in publishers {
        if layout.names.contains(&publisher.name) {
            return Err(format!("duplicate publisher {}", publisher.name).into());
        }
        layout.names.push(publisher.name);
        for address in &publisher.resources.listeners {
            layout.listener(publisher.name, *address)?;
        }
        for path in &publisher.resources.writable_paths {
            layout.writable_path(publisher.name, path)?;
        }
    }
    Ok(())
}

fn is_private(address: SocketAddr) -> bool {
    match address.ip() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    }
}

impl Layout {
    /// A fixed private listener, used by one owner once.
    fn listener(&mut self, owner: &'static str, address: SocketAddr) -> Result<()> {
        if !is_private(address) || address.port() == 0 {
            return Err(format!(
                "{owner} listener {address} needs a loopback or private (RFC 1918, ULA) address with a fixed port"
            )
            .into());
        }
        if let Some((_, other)) = self.listeners.iter().find(|(existing, _)| {
            *existing == address
                || (existing.port() == address.port()
                    && (existing.ip().is_unspecified() || address.ip().is_unspecified()))
        }) {
            return Err(
                format!("{owner} listener {address} conflicts with the {other} listener").into(),
            );
        }
        self.listeners.push((address, owner));
        Ok(())
    }

    /// An absolute path, not overlapping another owner's after resolving the
    /// prefixes that exist.
    fn writable_path(&mut self, owner: &'static str, root: &Path) -> Result<()> {
        if !root.is_absolute() {
            return Err(format!(
                "{owner} state directory {} must be absolute",
                root.display()
            )
            .into());
        }
        let path = normalize(root).map_err(|error| {
            format!(
                "cannot resolve {owner} state directory {}: {error}",
                root.display()
            )
        })?;
        if let Some((other, existing)) = self.writable_paths.iter().find(|(other, existing)| {
            *other != owner && (path.starts_with(existing) || existing.starts_with(&path))
        }) {
            return Err(format!(
                "{owner} state directory {} overlaps the {other} state directory {}",
                path.display(),
                existing.display()
            )
            .into());
        }
        self.writable_paths.push((owner, path));
        Ok(())
    }
}

/// Resolve each existing prefix before interpreting a later `..`.
fn normalize(path: &Path) -> std::io::Result<PathBuf> {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                result.pop();
            }
            Component::CurDir => {}
            _ => {
                result.push(component);
                if result.exists() {
                    result = result.canonicalize()?;
                }
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pir_pipeline::publisher::PublisherResources;

    fn definition(
        name: &'static str,
        listeners: &[&str],
        paths: &[&Path],
    ) -> PublisherDefinition<axum::Router> {
        let resources = PublisherResources {
            listeners: listeners.iter().map(|a| a.parse().unwrap()).collect(),
            writable_paths: paths.iter().map(|p| p.to_path_buf()).collect(),
            probes: Vec::new(),
        };
        PublisherDefinition::new(name, resources, |_| async { Ok(()) })
    }

    fn management() -> SocketAddr {
        "127.0.0.1:9400".parse().unwrap()
    }

    fn error(publishers: &[PublisherDefinition<axum::Router>]) -> String {
        validate(management(), publishers).unwrap_err().to_string()
    }

    #[test]
    fn disjoint_private_resources_pass() {
        let temp = tempfile::tempdir().unwrap();
        let publishers = [
            definition("memo", &["127.0.0.1:9401"], &[&temp.path().join("memo")]),
            definition(
                "tachygram",
                &["10.0.0.5:9402"],
                &[&temp.path().join("tachygram")],
            ),
            definition(
                "registry",
                &["[fd00::1]:9403"],
                &[&temp.path().join("registry")],
            ),
        ];
        validate(management(), &publishers).unwrap();
    }

    #[test]
    fn management_must_be_loopback_with_a_fixed_port() {
        for address in ["10.0.0.5:9400", "0.0.0.0:9400", "127.0.0.1:0"] {
            assert!(
                validate(address.parse().unwrap(), &[]).is_err(),
                "{address}"
            );
        }
    }

    #[test]
    fn product_listeners_must_be_private_fixed_and_unshared() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("a");
        for address in [
            "8.8.8.8:9401",
            "0.0.0.0:9401",
            "[2001:db8::1]:9401",
            "127.0.0.1:0",
        ] {
            let message = error(&[definition("tachygram", &[address], &[&dir])]);
            assert!(message.contains("fixed port"), "{address}: {message}");
        }
        let message = error(&[definition("tachygram", &["127.0.0.1:9400"], &[&dir])]);
        assert!(
            message.contains("conflicts with the management listener"),
            "{message}"
        );
        let message = error(&[
            definition("memo", &["127.0.0.1:9401"], &[&temp.path().join("m")]),
            definition("tachygram", &["127.0.0.1:9401"], &[&temp.path().join("t")]),
        ]);
        assert!(
            message.contains("conflicts with the memo listener"),
            "{message}"
        );
    }

    #[test]
    fn state_directories_must_be_absolute_and_disjoint() {
        let temp = tempfile::tempdir().unwrap();
        let message = error(&[definition(
            "tachygram",
            &["127.0.0.1:9402"],
            &[Path::new("state")],
        )]);
        assert!(message.contains("must be absolute"), "{message}");

        let shared = temp.path().join("state");
        let nested = shared.join("tachygram");
        let message = error(&[
            definition("memo", &["127.0.0.1:9401"], &[&shared]),
            definition("tachygram", &["127.0.0.1:9402"], &[&nested]),
        ]);
        assert!(
            message.contains("overlaps the memo state directory"),
            "{message}"
        );

        // An alias through `..` and an existing symlink is still an overlap.
        std::fs::create_dir_all(&shared).unwrap();
        let link = temp.path().join("link");
        std::os::unix::fs::symlink(&shared, &link).unwrap();
        let dotted = temp.path().join("other").join("..").join("state");
        for alias in [link.as_path(), dotted.as_path()] {
            let message = error(&[
                definition("memo", &["127.0.0.1:9401"], &[&shared]),
                definition("tachygram", &["127.0.0.1:9402"], &[alias]),
            ]);
            assert!(
                message.contains("overlaps"),
                "{}: {message}",
                alias.display()
            );
        }
    }

    #[test]
    fn publisher_names_are_unique() {
        let temp = tempfile::tempdir().unwrap();
        let message = error(&[
            definition("tachygram", &["127.0.0.1:9401"], &[&temp.path().join("a")]),
            definition("tachygram", &["127.0.0.1:9402"], &[&temp.path().join("b")]),
        ]);
        assert!(
            message.contains("duplicate publisher tachygram"),
            "{message}"
        );
    }
}
