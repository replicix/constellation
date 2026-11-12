//! `StorageClass` parameters (plan 37 §6): the parsing and validation live
//! in `replicix_csi_common::params`; what stays here is Constellation's
//! default data location and the conversion to the control protocol's
//! `fs.create` parameters.

use constellation_control::proto::types::FsCreateParams;
use replicix_csi_common::params::ClassParams;

/// The bucket prefix a class gets without an explicit `prefix`. §6 wants
/// `constellation-csi/<storageclass-name>`, but `CreateVolumeRequest` does
/// not carry the class name (external-provisioner's
/// `--extra-create-metadata` adds the PVC and PV names only), so the
/// default is one shared prefix: two classes sharing a bucket must set
/// distinct prefixes, or they share (or, with different filesystem
/// parameters, conflict on) one pool. It is a data location: it must
/// never change.
pub const DEFAULT_PREFIX: &str = "constellation-csi";

/// `(bucket, prefix)` with matching parameters.
pub fn fs_create_params(class: &ClassParams, prefix: String) -> FsCreateParams {
    let s = class.fs_create_spec(prefix);
    FsCreateParams {
        name: s.name,
        bucket: s.bucket,
        prefix: s.prefix,
        endpoint: s.endpoint,
        region: s.region,
        chunk_size: s.chunk_size,
        compression: s.compression,
        e2e: s.e2e,
        write_mode: s.write_mode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn params(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_class_without_a_prefix_gets_the_constellation_location() {
        assert_eq!(DEFAULT_PREFIX, "constellation-csi");
        let class = ClassParams::parse(&params(&[("bucket", "b")]), DEFAULT_PREFIX).unwrap();
        assert_eq!(class.pool_prefix(0), "constellation-csi");
        let fs = fs_create_params(&class, class.pool_prefix(0));
        assert_eq!(fs.bucket, "b");
        assert_eq!(fs.prefix, "constellation-csi");
        assert_eq!(fs.name, None);
    }
}
