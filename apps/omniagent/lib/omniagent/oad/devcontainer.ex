defmodule Omniagent.Oad.Devcontainer do
  @moduledoc """
  Minimal devcontainer.json parser (Elixir port of the supported subset).

  Extracts `image`, `containerEnv`/`remoteEnv`, `workspaceFolder`, and flattens
  the lifecycle hooks into two shell scripts: a *create* script
  (`onCreate` → `updateContent` → `postCreate`) baked into the workspace snapshot
  at build time, and a *start* script (`postStart` → `postAttach`) run per session
  by `serve-session`. Unsupported fields (`features`, `build`/`dockerfile`,
  `forwardPorts`, `mounts`, …) are collected for a warning.

  Tolerates JSONC: `//` and `/* */` comments and trailing commas.
  """

  defstruct image: nil,
            workspace_folder: nil,
            container_env: %{},
            remote_env: %{},
            create_commands: [],
            start_commands: [],
            unsupported: []

  @known ~w(image name workspaceFolder containerEnv remoteEnv onCreateCommand
            updateContentCommand postCreateCommand postStartCommand
            postAttachCommand customizations remoteUser containerUser)

  @unsupported ~w(build dockerFile dockerfile features forwardPorts appPort
                  portsAttributes mounts runArgs dockerComposeFile)

  @create_fields ~w(onCreateCommand updateContentCommand postCreateCommand)
  @start_fields ~w(postStartCommand postAttachCommand)

  @doc "Parses devcontainer JSON (JSONC tolerated). Returns `{:ok, t}` or `{:error, reason}`."
  def parse(text) when is_binary(text) do
    case Jason.decode(strip_jsonc(text)) do
      {:ok, json} when is_map(json) -> {:ok, from_map(json)}
      {:ok, _} -> {:error, :not_an_object}
      {:error, %Jason.DecodeError{} = err} -> {:error, Exception.message(err)}
    end
  end

  @doc "Builds the parsed struct from an already-decoded JSON map."
  def from_map(json) when is_map(json) do
    %__MODULE__{
      image: json["image"],
      workspace_folder: json["workspaceFolder"],
      container_env: string_map(json["containerEnv"]),
      remote_env: string_map(json["remoteEnv"]),
      create_commands: lifecycle(json, @create_fields),
      start_commands: lifecycle(json, @start_fields),
      unsupported: unsupported_fields(json)
    }
  end

  @doc "Combined create-time script, or nil when there are no create hooks."
  def create_script(%__MODULE__{create_commands: cmds}), do: combine(cmds)

  @doc "Combined per-session start script, or nil when there are none."
  def start_script(%__MODULE__{start_commands: cmds}), do: combine(cmds)

  defp lifecycle(json, fields) do
    fields
    |> Enum.map(fn field -> command_to_shell(field, json[field]) end)
    |> Enum.reject(&is_nil/1)
  end

  defp combine([]), do: nil

  defp combine(commands) do
    body =
      Enum.map_join(commands, "", fn %{source: source, command: command} ->
        "echo '[omniagent] #{source}'\n#{command}\n"
      end)

    "set -e\n" <> body
  end

  defp command_to_shell(_field, nil), do: nil

  defp command_to_shell(field, value) do
    case stringify(value) do
      nil -> nil
      command -> %{source: field, command: command}
    end
  end

  # string → verbatim; [argv] → space-joined; {name: cmd} → sorted, newline-joined.
  defp stringify(value) when is_binary(value) do
    if String.trim(value) == "", do: nil, else: value
  end

  defp stringify(value) when is_list(value) do
    case Enum.filter(value, &is_binary/1) do
      [] -> nil
      parts -> Enum.join(parts, " ")
    end
  end

  defp stringify(value) when is_map(value) do
    lines =
      value
      |> Enum.sort_by(fn {k, _} -> k end)
      |> Enum.map(fn {_, v} -> stringify(v) end)
      |> Enum.reject(&is_nil/1)

    case lines do
      [] -> nil
      _ -> Enum.join(lines, "\n")
    end
  end

  defp stringify(_), do: nil

  defp string_map(nil), do: %{}

  defp string_map(map) when is_map(map) do
    for {k, v} <- map, is_binary(v), into: %{}, do: {k, v}
  end

  defp string_map(_), do: %{}

  defp unsupported_fields(json) do
    json
    |> Map.keys()
    |> Enum.filter(fn key -> key in @unsupported or key not in @known end)
    |> Enum.sort()
    |> Enum.uniq()
  end

  # --- JSONC stripping (string-aware) ---

  defp strip_jsonc(text), do: scan(text, :normal, [])

  defp scan(<<>>, _state, acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()

  # string body
  defp scan(<<"\\", rest::binary>>, :string, acc), do: scan(rest, :string_escape, ["\\" | acc])
  defp scan(<<"\"", rest::binary>>, :string, acc), do: scan(rest, :normal, ["\"" | acc])
  defp scan(<<c::utf8, rest::binary>>, :string, acc), do: scan(rest, :string, [<<c::utf8>> | acc])

  # escaped char inside a string
  defp scan(<<c::utf8, rest::binary>>, :string_escape, acc),
    do: scan(rest, :string, [<<c::utf8>> | acc])

  # line comment: drop until newline (keep the newline)
  defp scan(<<"\n", rest::binary>>, :line_comment, acc), do: scan(rest, :normal, ["\n" | acc])
  defp scan(<<_c::utf8, rest::binary>>, :line_comment, acc), do: scan(rest, :line_comment, acc)

  # block comment: drop until */
  defp scan(<<"*/", rest::binary>>, :block_comment, acc), do: scan(rest, :normal, acc)
  defp scan(<<_c::utf8, rest::binary>>, :block_comment, acc), do: scan(rest, :block_comment, acc)

  # normal
  defp scan(<<"\"", rest::binary>>, :normal, acc), do: scan(rest, :string, ["\"" | acc])
  defp scan(<<"//", rest::binary>>, :normal, acc), do: scan(rest, :line_comment, acc)
  defp scan(<<"/*", rest::binary>>, :normal, acc), do: scan(rest, :block_comment, acc)

  defp scan(<<",", rest::binary>>, :normal, acc) do
    if trailing_comma?(rest),
      do: scan(rest, :normal, acc),
      else: scan(rest, :normal, ["," | acc])
  end

  defp scan(<<c::utf8, rest::binary>>, :normal, acc), do: scan(rest, :normal, [<<c::utf8>> | acc])

  defp trailing_comma?(rest) do
    case String.trim_leading(rest) do
      <<"}", _::binary>> -> true
      <<"]", _::binary>> -> true
      "}" -> true
      "]" -> true
      _ -> false
    end
  end
end
