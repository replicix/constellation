//! [`Report`]: what a conformance run found, readable in CI output and
//! machine-readable for the parity checker to extend to later.

use super::Group;
use crate::caps::Cap;

/// One test's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    /// Failed, with the failure message.
    Failed(String),
    /// Not applicable to this target, with the reason (a missing
    /// capability is named as a whole word: `requires capability X`).
    Skipped(String),
}

/// One test's line of a [`Report`].
#[derive(Debug, Clone)]
pub struct TestReport {
    pub name: &'static str,
    pub group: Group,
    pub outcome: Outcome,
    pub seconds: f64,
}

impl TestReport {
    /// `group::name`.
    pub fn id(&self) -> String {
        format!("{}::{}", self.group.name(), self.name)
    }
}

/// A run's results.
#[derive(Debug, Clone)]
pub struct Report {
    pub target: String,
    /// The capabilities the run was made as.
    pub caps: Vec<Cap>,
    pub seed: u64,
    pub tests: Vec<TestReport>,
}

impl Report {
    pub fn passed(&self) -> Vec<&TestReport> {
        self.select(|o| matches!(o, Outcome::Passed))
    }

    pub fn failed(&self) -> Vec<&TestReport> {
        self.select(|o| matches!(o, Outcome::Failed(_)))
    }

    pub fn skipped(&self) -> Vec<&TestReport> {
        self.select(|o| matches!(o, Outcome::Skipped(_)))
    }

    fn select(&self, f: impl Fn(&Outcome) -> bool) -> Vec<&TestReport> {
        self.tests.iter().filter(|t| f(&t.outcome)).collect()
    }

    /// Nothing failed.
    pub fn is_ok(&self) -> bool {
        self.failed().is_empty()
    }

    /// The outcome of the test `group::name`.
    pub fn outcome(&self, id: &str) -> Option<&Outcome> {
        self.tests.iter().find(|t| t.id() == id).map(|t| &t.outcome)
    }

    /// A table for CI output: one line per test, failures with their
    /// message, a summary last.
    pub fn render(&self) -> String {
        let mut out = format!(
            "conformance: {} (seed {:#x}, caps: {})\n",
            self.target,
            self.seed,
            self.caps
                .iter()
                .map(|c| c.name())
                .collect::<Vec<_>>()
                .join(" ")
        );
        for t in &self.tests {
            let (tag, note) = match &t.outcome {
                Outcome::Passed => ("PASS", String::new()),
                Outcome::Failed(msg) => ("FAIL", format!("  {msg}")),
                Outcome::Skipped(why) => ("SKIP", format!("  ({why})")),
            };
            out.push_str(&format!(
                "  {tag}  {:<58} {:>6.2}s{note}\n",
                t.id(),
                t.seconds
            ));
        }
        out.push_str(&format!(
            "  {} passed, {} failed, {} skipped\n",
            self.passed().len(),
            self.failed().len(),
            self.skipped().len()
        ));
        out
    }

    /// The report in the harness's results-file shape (schema 1,
    /// `tests/parity.py`'s input): scenarios named `conformance/<group>::
    /// <name>`, with `lane` the target's name.
    pub fn to_json(&self) -> String {
        fn esc(s: &str) -> String {
            let mut out = String::with_capacity(s.len() + 2);
            out.push('"');
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                    c => out.push(c),
                }
            }
            out.push('"');
            out
        }
        let scenarios: Vec<String> = self
            .tests
            .iter()
            .map(|t| {
                let (outcome, reason) = match &t.outcome {
                    Outcome::Passed => ("passed", "null".to_string()),
                    Outcome::Failed(m) => ("failed", esc(m)),
                    Outcome::Skipped(m) => ("skipped", esc(m)),
                };
                format!(
                    "{{\"name\":{},\"outcome\":\"{outcome}\",\"seconds\":{:.3},\"reason\":{reason}}}",
                    esc(&format!("conformance/{}", t.id())),
                    t.seconds
                )
            })
            .collect();
        format!(
            "{{\"schema\":1,\"lane\":{},\"seed\":{},\"scenarios\":[{}]}}",
            esc(&self.target),
            self.seed,
            scenarios.join(",")
        )
    }

    /// Panic with the failures if any test failed.
    #[track_caller]
    pub fn assert_ok(&self) {
        if !self.is_ok() {
            panic!("conformance failures:\n{}", self.render());
        }
    }
}
