defmodule Omniagent.Accounts.ApiToken do
  use Ecto.Schema
  import Ecto.Changeset

  alias Omniagent.Accounts.User

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "api_tokens" do
    field :description, :string
    field :token_hash, :string
    field :last_used_at, :utc_datetime_usec
    belongs_to :user, User

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(token, attrs) do
    token
    |> cast(attrs, [:description, :token_hash, :last_used_at, :user_id])
    |> validate_required([:token_hash, :user_id])
    |> unique_constraint(:token_hash)
  end
end
