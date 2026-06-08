defmodule Omniagent.Artifacts.Artifact do
  use Ecto.Schema
  import Ecto.Changeset

  alias Omniagent.Sessions.AgentSession

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "artifacts" do
    field :kind, :string
    field :bucket, :string
    field :key, :string
    field :content_type, :string
    field :size, :integer
    field :checksum, :string
    field :metadata, :map, default: %{}
    belongs_to :agent_session, AgentSession

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(artifact, attrs) do
    artifact
    |> cast(attrs, [
      :kind,
      :bucket,
      :key,
      :content_type,
      :size,
      :checksum,
      :metadata,
      :agent_session_id
    ])
    |> validate_required([:kind, :bucket, :key, :agent_session_id])
    |> unique_constraint([:bucket, :key], name: :artifacts_bucket_key_index)
  end
end
