defmodule OmniagentWeb.CliController do
  @moduledoc """
  Serves the pre-built `omniagent` CLI binaries that ship inside the release
  image, keyed by operating system and architecture.

  The binaries live in a bundle directory laid out as `<os>-<arch>/omniagent`.
  In the container image that directory is `/cli` (see `CLI_BUNDLE_DIR`); the
  fallback covers non-container runs where the binaries were copied into the
  web app's `priv/static/cli`.

  Serving them through a controller (rather than `Plug.Static`) keeps the
  download URLs stable — `Plug.Static` only exposes the paths in
  `OmniagentWeb.static_paths/0`, and `phx.digest` would otherwise fingerprint
  the filenames.
  """
  use OmniagentWeb, :controller

  # {os, arch} from the URL -> bundle subdirectory name.
  @targets %{
    {"linux", "x86_64"} => "linux-x86_64",
    {"linux", "aarch64"} => "linux-aarch64",
    {"darwin", "aarch64"} => "darwin-aarch64"
  }

  def download(conn, %{"os" => os, "arch" => arch}) do
    with {:ok, dir} <- Map.fetch(@targets, {os, arch}),
         path = Path.join([bundle_dir(), dir, "omniagent"]),
         true <- File.regular?(path) do
      conn
      |> put_resp_content_type("application/octet-stream")
      |> put_resp_header("content-disposition", ~s(attachment; filename="omniagent"))
      |> send_file(200, path)
    else
      :error -> send_resp(conn, 404, "unknown target #{os}/#{arch}")
      false -> send_resp(conn, 404, "binary not available for #{os}/#{arch}")
    end
  end

  defp bundle_dir do
    System.get_env("CLI_BUNDLE_DIR") ||
      Application.app_dir(:omniagent_web, "priv/static/cli")
  end
end
