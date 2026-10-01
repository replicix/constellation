//! `constellation-csi`: the Kubernetes CSI driver (plan 37). Identity,
//! Controller and Node gRPC services, and the [`control_client`] seam those
//! services drive against engine pods over the control protocol.

pub mod control_client;
pub mod controller;
pub mod identity;
pub mod node;
pub mod proto;
