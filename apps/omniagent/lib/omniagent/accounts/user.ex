defmodule Omniagent.Accounts.User do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "users" do
    field :email, :string
    field :hashed_password, :string
    field :role, :string, default: "admin"

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(user, attrs) do
    user
    |> cast(attrs, [:email, :hashed_password, :role])
    |> validate_required([:email])
    |> update_change(:email, &String.downcase/1)
    |> unique_constraint(:email)
  end
end
