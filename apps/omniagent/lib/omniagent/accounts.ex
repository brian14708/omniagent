defmodule Omniagent.Accounts do
  @moduledoc """
  Minimal account and API-token context for OmniAgent's central server.

  API tokens are stored as SHA-256 hex digests. The raw token is only shown or
  supplied by an operator/CLI and is never persisted.
  """

  import Ecto.Query

  alias Omniagent.Accounts.{ApiToken, User}
  alias Omniagent.Repo

  def get_user(id), do: Repo.get(User, id)

  @doc """
  Returns the default operator user, looked up by `OMNIAGENT_ADMIN_EMAIL`
  (the same env the seeds use), or `nil` if it has not been seeded yet.
  """
  def default_user do
    System.get_env("OMNIAGENT_ADMIN_EMAIL", "admin@omniagent.local")
    |> get_user_by_email()
  end

  def get_user_by_email(email) when is_binary(email) do
    Repo.get_by(User, email: String.downcase(email))
  end

  def create_user(attrs) do
    %User{}
    |> User.changeset(attrs)
    |> Repo.insert()
  end

  def get_or_create_user(email, attrs \\ %{}) when is_binary(email) do
    case get_user_by_email(email) do
      nil -> create_user(Map.merge(attrs, %{email: email}))
      user -> {:ok, user}
    end
  end

  def create_api_token(%User{} = user, raw_token, attrs \\ %{}) when is_binary(raw_token) do
    %ApiToken{}
    |> ApiToken.changeset(%{
      user_id: user.id,
      token_hash: token_hash(raw_token),
      description: Map.get(attrs, :description) || Map.get(attrs, "description")
    })
    |> Repo.insert()
  end

  def verify_api_token(nil), do: {:error, :missing_token}
  def verify_api_token(""), do: {:error, :missing_token}

  def verify_api_token(raw_token) when is_binary(raw_token) do
    hash = token_hash(raw_token)

    query =
      from token in ApiToken,
        where: token.token_hash == ^hash,
        preload: [:user]

    case Repo.one(query) do
      nil ->
        {:error, :invalid_token}

      token ->
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        token |> ApiToken.changeset(%{last_used_at: now}) |> Repo.update()
        {:ok, token.user, token}
    end
  end

  def token_hash(token) when is_binary(token) do
    :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
  end
end
