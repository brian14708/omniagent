defmodule OmniagentWeb.OadController do
  @moduledoc """
  Accepts oad self-registration heartbeats.

  oad daemons `POST /api/oad/register` every ~15s, authenticating with a shared
  registration secret (`OMNIAGENT_OAD_REGISTER_TOKEN`). Each beat upserts the
  instance and refreshes its liveness; the control plane then calls the instance's
  `/v1` API directly. `DELETE /api/oad/register/:id` deregisters on shutdown.
  """

  use OmniagentWeb, :controller

  alias Omniagent.OadInstances
  alias Omniagent.Oad.Snapshots

  def register(conn, params) do
    if authorized?(conn) do
      attrs = %{
        "instance_id" => params["instance_id"],
        "name" => params["name"],
        "base_url" => params["advertise_url"] || params["base_url"],
        "api_token" => params["api_token"],
        "capabilities" => params["capabilities"] || %{},
        "version" => params["version"]
      }

      case OadInstances.register(attrs) do
        {:ok, instance} ->
          json(conn, %{ok: true, instance_id: instance.instance_id})

        {:error, _changeset} ->
          conn |> put_status(:unprocessable_entity) |> json(%{error: "invalid registration"})
      end
    else
      unauthorized(conn)
    end
  end

  def delete(conn, %{"id" => id}) do
    if authorized?(conn) do
      OadInstances.deregister(id)
      send_resp(conn, :no_content, "")
    else
      unauthorized(conn)
    end
  end

  @doc """
  Batched chunk existence check. The daemon posts the chunk hashes it is about
  to upload and receives the subset the store is missing, so it transfers only
  the delta.
  """
  def cas_check(conn, params) do
    if authorized?(conn) do
      hashes = List.wrap(params["hashes"])
      json(conn, %{missing: Snapshots.missing_chunks(hashes)})
    else
      unauthorized(conn)
    end
  end

  defp authorized?(conn) do
    expected = Application.get_env(:omniagent, :oad_register_token)

    case get_req_header(conn, "authorization") do
      ["Bearer " <> token] ->
        is_binary(expected) and expected != "" and Plug.Crypto.secure_compare(token, expected)

      _ ->
        false
    end
  end

  defp unauthorized(conn) do
    conn |> put_status(:unauthorized) |> json(%{error: "unauthorized"})
  end
end
