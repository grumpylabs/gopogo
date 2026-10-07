{{/* Chart name. */}}
{{- define "gopogo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified app name. */}}
{{- define "gopogo.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "gopogo.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "gopogo.labels" -}}
helm.sh/chart: {{ include "gopogo.chart" . }}
{{ include "gopogo.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "gopogo.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gopogo.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "gopogo.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "gopogo.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Name of the Secret holding the auth password. */}}
{{- define "gopogo.authSecretName" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecret }}
{{- else }}
{{- printf "%s-auth" (include "gopogo.fullname" .) }}
{{- end }}
{{- end }}

{{- define "gopogo.authSecretKey" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecretKey }}
{{- else -}}
password
{{- end }}
{{- end }}

{{/* gopogo command-line flags. */}}
{{- define "gopogo.args" -}}
- --host=0.0.0.0
- --port={{ .Values.port }}
- --redis={{ .Values.protocols.redis }}
- --http={{ .Values.protocols.http }}
- --memcache={{ .Values.protocols.memcache }}
- --postgres={{ .Values.protocols.postgres }}
- --maxmemory={{ .Values.cache.maxMemory }}
{{- if not .Values.cache.evict }}
- --noevict
{{- end }}
{{- if not .Values.cache.sixpack }}
- --nosixpack
{{- end }}
- --shards={{ .Values.cache.shards }}
- --loadfactor={{ .Values.cache.loadFactor }}
- --cas={{ .Values.cache.cas }}
- --autosweep={{ .Values.cache.autoSweep }}
- --sweepinterval={{ .Values.cache.sweepInterval }}
- --maxconns={{ .Values.maxConns }}
{{- if .Values.tls.enabled }}
- --tlsport={{ .Values.tls.port }}
- --tlscert=/etc/gopogo/tls/tls.crt
- --tlskey=/etc/gopogo/tls/tls.key
{{- if .Values.tls.verifyClients }}
- --tlscacert=/etc/gopogo/tls/ca.crt
{{- end }}
{{- end }}
{{- if .Values.persistence.enabled }}
- --persist=/data/{{ .Values.persistence.fileName }}
{{- end }}
{{- if .Values.telemetry.enabled }}
- --telemetry
{{- /* Options added in later chart versions may be absent when upgrading with
     --reuse-values, so each has a fallback (hasKey keeps an explicit false or 0). */}}
{{- $t := .Values.telemetry }}
- --telemetry-exporter={{ $t.exporter | default "otlp" }}
{{- with $t.metricsExporter }}
- --metrics-exporter={{ . }}
{{- end }}
{{- with $t.tracesExporter }}
- --traces-exporter={{ . }}
{{- end }}
{{- with $t.protocol }}
- --otlp-protocol={{ . }}
{{- end }}
{{- with $t.otlpEndpoint }}
- --otlp-endpoint={{ . }}
{{- end }}
- --otlp-insecure={{ if hasKey $t "insecure" }}{{ $t.insecure }}{{ else }}true{{ end }}
{{- with $t.environment }}
- --telemetry-environment={{ . }}
{{- end }}
{{- if and (hasKey $t "traceSampleRatio") (not (kindIs "invalid" $t.traceSampleRatio)) }}
{{- if ne (toString $t.traceSampleRatio) "" }}
- --trace-sample-ratio={{ $t.traceSampleRatio }}
{{- end }}
{{- end }}
{{- end }}
{{- if .Values.verbose }}
- --verbose
{{- end }}
- --log-level={{ .Values.logLevel | default "info" }}
{{- range .Values.extraArgs }}
- {{ . | quote }}
{{- end }}
{{- end }}

{{/*
OpenTelemetry resource attributes for the pod, from the downward API, plus
telemetry.clusterName, telemetry.serviceNamespace and
telemetry.resourceAttributes. Kubernetes expands $(VAR) from the variables
defined before it. An attribute also set by the image's
OTEL_RESOURCE_ATTRIBUTES (e.g. via extraEnv) takes the later value.
*/}}
{{- define "gopogo.resourceEnv" -}}
{{- $t := .Values.telemetry -}}
{{- $attrs := list
  "k8s.pod.name=$(K8S_POD_NAME)"
  "k8s.pod.uid=$(K8S_POD_UID)"
  "k8s.namespace.name=$(K8S_NAMESPACE_NAME)"
  "k8s.node.name=$(K8S_NODE_NAME)"
  "k8s.container.name=gopogo"
  "service.instance.id=$(K8S_POD_NAME)"
-}}
{{- if .Values.persistence.enabled }}
{{- $attrs = append $attrs (printf "k8s.statefulset.name=%s" (include "gopogo.fullname" .)) -}}
{{- else }}
{{- $attrs = append $attrs (printf "k8s.deployment.name=%s" (include "gopogo.fullname" .)) -}}
{{- end }}
{{- with $t.clusterName }}
{{- $attrs = append $attrs (printf "k8s.cluster.name=%s" .) -}}
{{- end }}
{{- with $t.serviceNamespace }}
{{- $attrs = append $attrs (printf "service.namespace=%s" .) -}}
{{- end }}
{{- range $k, $v := $t.resourceAttributes }}
{{- $attrs = append $attrs (printf "%s=%s" $k (toString $v)) -}}
{{- end }}
- name: K8S_POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: K8S_POD_UID
  valueFrom:
    fieldRef:
      fieldPath: metadata.uid
- name: K8S_NAMESPACE_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: K8S_NODE_NAME
  valueFrom:
    fieldRef:
      fieldPath: spec.nodeName
- name: OTEL_RESOURCE_ATTRIBUTES
  value: {{ join "," $attrs | quote }}
{{- end }}
