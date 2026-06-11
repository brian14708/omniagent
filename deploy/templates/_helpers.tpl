{{- define "omniagent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "omniagent.fullname" -}}
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

{{- define "omniagent.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "omniagent.labels" -}}
helm.sh/chart: {{ include "omniagent.chart" . }}
{{ include "omniagent.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "omniagent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "omniagent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "omniagent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "omniagent.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "omniagent.secretName" -}}
{{- default (include "omniagent.fullname" .) .Values.secrets.existingSecret -}}
{{- end -}}

{{- define "omniagent.configMapName" -}}
{{- printf "%s-env" (include "omniagent.fullname" .) -}}
{{- end -}}

{{- define "omniagent.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{- define "omniagent.env" -}}
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
  - name: RELEASE_NODE
    value: "{{ .Values.cluster.nodeName }}@$(POD_IP)"
{{- with .Values.extraEnv }}
{{ toYaml . | nindent 2 }}
{{- end }}
envFrom:
  - configMapRef:
      name: {{ include "omniagent.configMapName" . }}
  - secretRef:
      name: {{ include "omniagent.secretName" . }}
{{- end -}}
