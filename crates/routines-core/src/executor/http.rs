use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::context::Context;
use crate::error::Result;

use super::{StepResult, StepStatus};

/// Execute an HTTP request step.
pub(super) fn execute(
    step_id: &str,
    url_template: &str,
    method_template: &str,
    headers: &HashMap<String, String>,
    body_template: Option<&str>,
    timeout: Option<u64>,
    ctx: &Context,
) -> Result<StepResult> {
    let url = ctx.resolve(url_template, step_id)?;
    let method = ctx.resolve(method_template, step_id)?;

    let start = Instant::now();

    // Build agent with timeout
    let config = if let Some(timeout_secs) = timeout {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(timeout_secs)))
            .build()
    } else {
        ureq::config::Config::default()
    };
    let agent = ureq::Agent::new_with_config(config);

    // Resolve headers
    let mut resolved_headers: Vec<(String, String)> = Vec::new();
    for (key, val_template) in headers {
        let val = ctx.resolve(val_template, step_id)?;
        resolved_headers.push((key.clone(), val));
    }

    // Resolve body
    let body_data = match body_template {
        Some(tmpl) => Some(ctx.resolve(tmpl, step_id)?),
        None => None,
    };

    // Build and send request
    let send_result = (|| -> std::result::Result<(u16, String), ureq::Error> {
        let method_upper = method.to_uppercase();

        macro_rules! apply_headers {
            ($builder:expr) => {{
                let mut req = $builder;
                for (k, v) in &resolved_headers {
                    req = req.header(k.as_str(), v.as_str());
                }
                req
            }};
        }

        let (status_code, body) = match method_upper.as_str() {
            "POST" => {
                let req = apply_headers!(agent.post(&url));
                let mut resp = if let Some(b) = &body_data {
                    req.send(b.as_bytes())?
                } else {
                    req.send_empty()?
                };
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            "PUT" => {
                let req = apply_headers!(agent.put(&url));
                let mut resp = if let Some(b) = &body_data {
                    req.send(b.as_bytes())?
                } else {
                    req.send_empty()?
                };
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            "PATCH" => {
                let req = apply_headers!(agent.patch(&url));
                let mut resp = if let Some(b) = &body_data {
                    req.send(b.as_bytes())?
                } else {
                    req.send_empty()?
                };
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            "DELETE" => {
                let req = apply_headers!(agent.delete(&url));
                let mut resp = req.call()?;
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            "HEAD" => {
                let req = apply_headers!(agent.head(&url));
                let mut resp = req.call()?;
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
            _ => {
                // Default to GET
                let req = apply_headers!(agent.get(&url));
                let mut resp = req.call()?;
                (
                    resp.status(),
                    resp.body_mut().read_to_string().unwrap_or_default(),
                )
            }
        };

        Ok((u16::from(status_code), body))
    })();

    let elapsed = start.elapsed().as_millis() as u64;

    match send_result {
        Ok((status_code, body)) => {
            let status_text = format!("HTTP {status_code} {method}");
            let success = (200..300).contains(&(status_code as i32));

            Ok(StepResult {
                step_id: step_id.to_string(),
                status: if success {
                    StepStatus::Success
                } else {
                    StepStatus::Failed
                },
                exit_code: Some(if success { 0 } else { 1 }),
                stdout: body,
                stderr: status_text,
                execution_time_ms: elapsed,
            })
        }
        Err(e) => Ok(StepResult {
            step_id: step_id.to_string(),
            status: StepStatus::Failed,
            exit_code: Some(1),
            stdout: String::new(),
            stderr: format!("HTTP error: {e}"),
            execution_time_ms: elapsed,
        }),
    }
}
