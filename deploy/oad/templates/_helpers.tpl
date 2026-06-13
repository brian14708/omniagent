{{- define "oad.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "oad.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "oad.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "oad.labels" -}}
helm.sh/chart: {{ include "oad.chart" . }}
{{ include "oad.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "oad.selectorLabels" -}}
app.kubernetes.io/name: {{ include "oad.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "oad.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "oad.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "oad.secretName" -}}
{{- default (include "oad.fullname" .) .Values.secrets.existingSecret -}}
{{- end -}}

{{- define "oad.configMapName" -}}
{{- printf "%s-env" (include "oad.fullname" .) -}}
{{- end -}}

{{- define "oad.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{/*
Pod env shared by both workload kinds. The advertised URL and instance name are
derived from the pod's own identity so the control plane can call this instance
directly by pod IP.
*/}}
{{- define "oad.env" -}}
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
  - name: NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName
  - name: OAD_ADVERTISE_URL
    value: "http://$(POD_IP):{{ .Values.http.port }}"
  {{- if not .Values.controlPlane.instanceName }}
  - name: OAD_INSTANCE_NAME
    value: "$(NODE_NAME)"
  {{- end }}
{{- with .Values.extraEnv }}
{{ toYaml . | nindent 2 }}
{{- end }}
envFrom:
  - configMapRef:
      name: {{ include "oad.configMapName" . }}
  - secretRef:
      name: {{ include "oad.secretName" . }}
{{- end -}}
