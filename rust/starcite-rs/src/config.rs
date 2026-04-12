use std::{env, net::SocketAddr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    None,
    UnsafeJwt,
}

pub const DEFAULT_LIST_LIMIT: u32 = 100;
pub const MAX_LIST_LIMIT: u32 = 1_000;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub ops_listen_addr: SocketAddr,
    pub database_url: String,
    pub max_connections: u32,
    pub migrate_on_boot: bool,
    pub auth_mode: AuthMode,
    pub telemetry_enabled: bool,
    pub session_runtime_idle_timeout_ms: u64,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = env::var("PORT").unwrap_or_else(|_| "4001".to_string());
        let ops_port = env::var("STARCITE_OPS_PORT").ok();
        let pprof_port = env::var("STARCITE_PPROF_PORT").ok();
        let (listen_addr, ops_listen_addr) =
            parse_listener_addrs(&host, &port, ops_port.as_deref(), pprof_port.as_deref())?;

        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@localhost:5433/starcite_rust_dev".to_string()
        });

        let max_connections = env::var("DATABASE_MAX_CONNECTIONS")
            .ok()
            .map(|raw| parse_positive_u32("DATABASE_MAX_CONNECTIONS", &raw))
            .transpose()?
            .unwrap_or(20);

        let migrate_on_boot = env::var("MIGRATE_ON_BOOT")
            .ok()
            .map(|raw| parse_bool("MIGRATE_ON_BOOT", &raw))
            .transpose()?
            .unwrap_or(true);

        let auth_mode = env::var("STARCITE_AUTH_MODE")
            .ok()
            .map(|raw| parse_auth_mode("STARCITE_AUTH_MODE", &raw))
            .transpose()?
            .unwrap_or(AuthMode::None);

        let telemetry_enabled = env::var("STARCITE_ENABLE_TELEMETRY")
            .ok()
            .map(|raw| parse_bool("STARCITE_ENABLE_TELEMETRY", &raw))
            .transpose()?
            .unwrap_or(true);

        let session_runtime_idle_timeout_ms = env::var("SESSION_RUNTIME_IDLE_TIMEOUT_MS")
            .ok()
            .map(|raw| parse_positive_u64("SESSION_RUNTIME_IDLE_TIMEOUT_MS", &raw))
            .transpose()?
            .unwrap_or(30_000);

        Ok(Self {
            listen_addr,
            ops_listen_addr,
            database_url,
            max_connections,
            migrate_on_boot,
            auth_mode,
            telemetry_enabled,
            session_runtime_idle_timeout_ms,
        })
    }
}

fn parse_positive_u32(name: &str, raw: &str) -> Result<u32, String> {
    match raw.trim().parse::<u32>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!("invalid positive integer for {name}: {raw}")),
    }
}

fn parse_bool(name: &str, raw: &str) -> Result<bool, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("invalid boolean for {name}: {raw}")),
    }
}

fn parse_positive_u64(name: &str, raw: &str) -> Result<u64, String> {
    match raw.trim().parse::<u64>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!("invalid positive integer for {name}: {raw}")),
    }
}

fn parse_auth_mode(name: &str, raw: &str) -> Result<AuthMode, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(AuthMode::None),
        "unsafe_jwt" => Ok(AuthMode::UnsafeJwt),
        _ => Err(format!("invalid auth mode for {name}: {raw}")),
    }
}

fn parse_listener_addrs(
    host: &str,
    port_raw: &str,
    ops_port_raw: Option<&str>,
    pprof_port_raw: Option<&str>,
) -> Result<(SocketAddr, SocketAddr), String> {
    let port = parse_port("PORT", port_raw)?;
    let listen_addr = parse_socket_addr(host, port)?;
    let ops_port = match ops_port_raw.or(pprof_port_raw) {
        Some(raw) => parse_port("STARCITE_OPS_PORT", raw)?,
        None => port
            .checked_add(1)
            .ok_or_else(|| format!("unable to derive STARCITE_OPS_PORT from PORT: {port}"))?,
    };
    let ops_listen_addr = parse_socket_addr(host, ops_port)?;

    if listen_addr == ops_listen_addr {
        return Err(format!(
            "ops listen address must differ from public listen address: {listen_addr}"
        ));
    }

    Ok((listen_addr, ops_listen_addr))
}

fn parse_port(name: &str, raw: &str) -> Result<u16, String> {
    match raw.trim().parse::<u16>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!("invalid port for {name}: {raw}")),
    }
}

fn parse_socket_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    format!("{host}:{port}")
        .parse()
        .map_err(|_| format!("invalid listen address: {host}:{port}"))
}

#[cfg(test)]
mod tests {
    use super::parse_listener_addrs;

    #[test]
    fn defaults_ops_port_to_public_port_plus_one() {
        let (listen, ops) =
            parse_listener_addrs("0.0.0.0", "4001", None, None).expect("ports should parse");

        assert_eq!(listen.to_string(), "0.0.0.0:4001");
        assert_eq!(ops.to_string(), "0.0.0.0:4002");
    }

    #[test]
    fn prefers_explicit_ops_port() {
        let (listen, ops) = parse_listener_addrs("127.0.0.1", "4001", Some("4100"), Some("4200"))
            .expect("ports should parse");

        assert_eq!(listen.to_string(), "127.0.0.1:4001");
        assert_eq!(ops.to_string(), "127.0.0.1:4100");
    }

    #[test]
    fn falls_back_to_pprof_port() {
        let (_, ops) = parse_listener_addrs("127.0.0.1", "4001", None, Some("4300"))
            .expect("ports should parse");

        assert_eq!(ops.to_string(), "127.0.0.1:4300");
    }

    #[test]
    fn rejects_ops_port_collision() {
        let error = parse_listener_addrs("127.0.0.1", "4001", Some("4001"), None)
            .expect_err("collision should fail");

        assert!(error.contains("ops listen address must differ"));
    }
}
