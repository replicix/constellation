//! `node.lifecycle` (plan 31 C8): push a host lifecycle event into the
//! host services' manual source — the one the engine subscribed to when it
//! started (`crate::lifecycle`) — and answer once the engine applied it,
//! with the lifecycle it left (a `Suspending`'s answer carries what the
//! suspension achieved by its deadline). Linux and macOS hosts have a
//! manual source; a host whose events come from the OS answers
//! `Unsupported`.

use super::EngineControl;
use constellation_control::proto::types::{LifecycleParams, LifecycleReport};
use constellation_control::proto::ControlError;

impl EngineControl {
    pub(crate) fn lifecycle(
        &self,
        params: &LifecycleParams,
    ) -> Result<LifecycleReport, ControlError> {
        let lifecycle = self.engine.lifecycle();
        let event = crate::lifecycle::event_from_spec(params.event);
        let applied = lifecycle
            .inject(&*self.engine.host().lifecycle, event)
            .map_err(|refused| match refused {
                crate::lifecycle::InjectRefused::NotManual => {
                    ControlError::unsupported(refused.to_string())
                }
                crate::lifecycle::InjectRefused::NotListening => {
                    ControlError::failed(refused.to_string())
                }
            })?;
        Ok(LifecycleReport {
            event: params.event,
            applied,
            status: lifecycle.status(),
        })
    }
}
