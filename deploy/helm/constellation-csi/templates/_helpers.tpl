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
