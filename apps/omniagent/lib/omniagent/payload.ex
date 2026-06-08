defmodule Omniagent.Payload do
  @moduledoc "Shared helpers for normalizing JSON payloads from clients into schema attrs."

  @doc """
  Fetches `key` from a payload that may use either string or atom keys,
  falling back to `default` when absent.

  Wire payloads arrive JSON-decoded with string keys; the atom fallback keeps
  internally-constructed (atom-keyed) maps working too.
  """
  def fetch(payload, key, default \\ nil) do
    payload[to_string(key)] || payload[key] || default
  end

  @doc "Coerces a value into a map, wrapping non-map values under a `\"value\"` key."
  def map_value(nil), do: %{}
  def map_value(value) when is_map(value), do: value
  def map_value(value), do: %{"value" => value}

  @doc "Parses an ISO8601 string (or passes through a `DateTime`) into a microsecond-truncated `DateTime`."
  def parse_datetime(nil), do: nil
  def parse_datetime(%DateTime{} = dt), do: dt

  def parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> DateTime.truncate(dt, :microsecond)
      _ -> nil
    end
  end

  def parse_datetime(_), do: nil
end
