alias Omniagent.Accounts

email = System.get_env("OMNIAGENT_ADMIN_EMAIL", "admin@omniagent.local")
token = System.get_env("OMNIAGENT_DEV_TOKEN", "dev-token")

{:ok, user} = Accounts.get_or_create_user(email, %{role: "admin"})

case Accounts.create_api_token(user, token, %{description: "development token"}) do
  {:ok, _api_token} ->
    IO.puts("Created OmniAgent API token for #{email}. Raw token: #{token}")

  {:error, changeset} ->
    if Keyword.has_key?(changeset.errors, :token_hash) do
      IO.puts("OmniAgent API token already exists for #{email}. Raw token remains: #{token}")
    else
      raise "failed to create API token: #{inspect(changeset.errors)}"
    end
end
