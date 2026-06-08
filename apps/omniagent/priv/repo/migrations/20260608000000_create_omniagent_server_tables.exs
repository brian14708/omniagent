defmodule Omniagent.Repo.Migrations.CreateOmniagentServerTables do
  use Ecto.Migration

  def change do
    create table(:users, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :email, :string, null: false
      add :hashed_password, :string
      add :role, :string, null: false, default: "admin"
      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:users, [:email])

    create table(:api_tokens, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :description, :string
      add :token_hash, :string, null: false
      add :last_used_at, :utc_datetime_usec
      add :user_id, references(:users, type: :binary_id, on_delete: :delete_all), null: false
      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:api_tokens, [:token_hash])
    create index(:api_tokens, [:user_id])

    create table(:agent_sessions, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :user_id, references(:users, type: :binary_id, on_delete: :delete_all), null: false
      add :name, :string
      add :status, :string, null: false, default: "offline"
      add :cwd, :text
      add :argv, {:array, :string}, null: false, default: []
      add :client_id, :string
      add :last_client_sequence, :bigint, null: false, default: 0
      add :connected_at, :utc_datetime_usec
      add :disconnected_at, :utc_datetime_usec
      add :metadata, :map, null: false, default: %{}
      timestamps(type: :utc_datetime_usec)
    end

    create index(:agent_sessions, [:user_id, :status])
    create index(:agent_sessions, [:client_id])

    create table(:session_events, primary_key: false) do
      add :id, :binary_id, primary_key: true

      add :agent_session_id,
          references(:agent_sessions, type: :binary_id, on_delete: :delete_all),
          null: false

      add :source, :string, null: false
      add :event_type, :string, null: false
      add :sequence, :bigint, null: false
      add :payload, :map, null: false, default: %{}
      add :occurred_at, :utc_datetime_usec
      timestamps(type: :utc_datetime_usec)
    end

    create index(:session_events, [:agent_session_id, :event_type])
    create unique_index(:session_events, [:agent_session_id, :source, :event_type, :sequence])

    create table(:trace_spans, primary_key: false) do
      add :id, :binary_id, primary_key: true

      add :agent_session_id,
          references(:agent_sessions, type: :binary_id, on_delete: :delete_all),
          null: false

      add :external_id, :string, null: false
      add :sequence, :bigint, null: false
      add :provider, :string, null: false
      add :model, :string
      add :method, :string
      add :request_base_url, :text
      add :upstream_base_url, :text
      add :path, :text, null: false
      add :streaming, :boolean, null: false, default: false
      add :request_headers, :map, null: false, default: %{}
      add :request, :map, null: false, default: %{}
      add :response_headers, :map, null: false, default: %{}
      add :response, :map, null: false, default: %{}
      add :stream_events, {:array, :map}, null: false, default: []
      add :usage, :map, null: false, default: %{}
      # Precomputed display tags from the proxy (e.g. result-type); rendered as
      # badges in the trace list so the UI need not reparse the response.
      add :labels, {:array, :map}, null: false, default: []
      add :status, :integer
      add :started_at, :utc_datetime_usec
      add :latency_ms, :bigint
      add :error, :text
      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:trace_spans, [:agent_session_id, :external_id])
    create unique_index(:trace_spans, [:agent_session_id, :sequence])

    create table(:review_items, primary_key: false) do
      add :id, :binary_id, primary_key: true

      add :agent_session_id,
          references(:agent_sessions, type: :binary_id, on_delete: :delete_all),
          null: false

      add :external_id, :string, null: false
      add :sequence, :bigint
      add :phase, :string, null: false
      add :attempt, :integer, null: false
      add :provider, :string, null: false
      add :model, :string
      add :method, :string
      add :path, :text, null: false
      add :streaming, :boolean, null: false, default: false
      add :request, :map, null: false, default: %{}
      add :response, :map, null: false, default: %{}
      add :usage, :map, null: false, default: %{}
      add :status, :integer
      add :latency_ms, :bigint
      add :started_at, :utc_datetime_usec
      add :error, :text
      add :decision, :map
      add :decided_at, :utc_datetime_usec
      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:review_items, [:agent_session_id, :external_id])
  end
end
