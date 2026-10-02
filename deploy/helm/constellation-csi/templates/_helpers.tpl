{{- define "csi.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{- end }}
{{- define "csi.sidecar" -}}{{ .root.Values.sidecars.registry }}/{{ .sc.image }}:{{ .sc.tag }}{{- end }}
{{- define "csi.labels" -}}
app.kubernetes.io/name: constellation-csi
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}
{{- define "csi.pluginDir" -}}{{ .Values.kubeletDir }}/plugins/{{ .Values.driverName }}{{- end }}
