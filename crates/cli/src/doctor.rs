//! `constellation doctor`'s report of what the provider answers at each
//! CAS edge, and of bucket versioning (plan 30 §M4 items 1 and 4). The
//! probes themselves are `constellation_store_s3::probe`; this module only
//! prints them and decides what is fatal.

use constellation_store_s3::{CasProbeReport, Versioning};

/// Print the probe report under `doctor`'s capability lines. Returns an
/// error when a probe found a precondition the provider did not enforce
/// atomically: every lease, segment and commit relies on exactly one
/// writer winning, so such a backend is unusable for more than one node.
pub fn print_cas_report(report: &CasProbeReport) -> anyhow::Result<()> {
    println!("conditional-write semantics (what this provider answers):");
    for probe in &report.probes {
        let verdict = if probe.violation {
            "VIOLATION"
        } else if probe.known {
            "ok"
        } else {
            "UNKNOWN"
        };
        println!("  {:<34} {verdict}: {}", probe.name, probe.observed);
    }
    for probe in report.unknown() {
        println!(
            "warning: `{}` answered with unknown semantics ({}); constellation may misjudge \
             a lost race or a retryable conflict on this provider. Please report the provider \
             and this line.",
            probe.name, probe.observed
        );
    }
    println!(
        "bucket versioning ................. {} (informational; nothing relies on it)",
        report.versioning.as_str()
    );
    if report.versioning == Versioning::Enabled {
        println!(
            "note: with versioning on, every object GC deletes stays as a noncurrent version \
             and keeps costing storage until a lifecycle rule expires noncurrent versions"
        );
    }
    let violations: Vec<&str> = report.violations().map(|p| p.name.as_str()).collect();
    if !violations.is_empty() {
        anyhow::bail!(
            "the backend did not enforce a conditional write atomically ({}); leases, log \
             segments and commits would not be exclusive — do not use it for more than one node",
            violations.join(", ")
        );
    }
    Ok(())
}

/// The same report, in the control API's shape.
pub fn api_probes(report: &CasProbeReport) -> Vec<constellation_api::CasProbeStatus> {
    report
        .probes
        .iter()
        .map(|p| constellation_api::CasProbeStatus {
            name: p.name.clone(),
            observed: p.observed.clone(),
            known: p.known,
            violation: p.violation,
        })
        .collect()
}
