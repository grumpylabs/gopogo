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
- --telemetry-exporter={{ .Values.telemetry.exporter }}
- --otlp-endpoint={{ .Values.telemetry.otlpEndpoint }}
{{- end }}
{{- range .Values.extraArgs }}
- {{ . | quote }}
{{- end }}
{{- end }}
