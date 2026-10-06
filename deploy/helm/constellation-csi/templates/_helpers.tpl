{{- define "csi.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end }}
{{- define "csi.sidecar" -}}{{ .root.Values.sidecars.registry }}/{{ .sc.image }}:{{ .sc.tag }}{{- end }}
{{- define "csi.labels" -}}
app.kubernetes.io/name: constellation-csi
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}
{{- define "csi.pluginDir" -}}{{ .Values.kubeletDir }}/plugins/{{ .Values.driverName }}{{- end }}
{{- /*
Plan 37 §9 `credentialSource: aws-default-chain`: the projected token the EKS
IRSA webhook (`aws-iam-token`, audience sts.amazonaws.com) or the EKS Pod
Identity webhook (`eks-pod-identity-token`, audience pods.eks.amazonaws.com)
injects into an engine pod whose ServiceAccount carries the annotation —
the one volume and mount besides the engine's own that the pod-access
policies admit (as CEL over `v`, a volume, and `m`, a volume mount).
*/}}
{{- define "csi.awsTokenVolume" -}}
(v.name == 'aws-iam-token' && has(v.projected)
              && v.projected.sources.all(s, has(s.serviceAccountToken)
                   && s.serviceAccountToken.?audience.orValue('') == 'sts.amazonaws.com'))
          || (v.name == 'eks-pod-identity-token' && has(v.projected)
              && v.projected.sources.all(s, has(s.serviceAccountToken)
                   && s.serviceAccountToken.?audience.orValue('') == 'pods.eks.amazonaws.com'))
{{- end }}
{{- define "csi.awsTokenMount" -}}
(m.name == 'aws-iam-token' && m.mountPath == '/var/run/secrets/eks.amazonaws.com/serviceaccount'
                       && m.?readOnly.orValue(false))
                   || (m.name == 'eks-pod-identity-token'
                       && m.mountPath == '/var/run/secrets/pods.eks.amazonaws.com/serviceaccount'
                       && m.?readOnly.orValue(false))
{{- end }}
{{- /*
A value as text, an integral number as an integer. Numbers reach the
templates as float64 from a values file, `--set-json` and every `helm
upgrade --reuse-values` (the release's values round-trip through JSON), and
`quote`/`toString` print a float64 past six digits in exponent form
(purge.bytesPerSecond 16777216 → "1.6777216e+07"), which the plugins refuse
to start on. Every number this chart renders is a count, a port or a
duration in whole units, so a fractional one is refused here, at render
time, rather than truncated.
*/}}
{{- define "csi.str" -}}
{{- if or (kindIs "float64" .) (kindIs "float32" .) -}}
{{- if ne (float64 (int64 .)) (float64 .) -}}
{{- fail (printf "constellation-csi: expected a whole number, got %v" .) -}}
{{- end -}}
{{- int64 . -}}
{{- else -}}
{{- toString . -}}
{{- end -}}
{{- end }}
