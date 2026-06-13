defmodule Omniagent.OadWorkspacesTest do
  use Omniagent.DataCase, async: true

  alias Omniagent.OadWorkspaces
  alias Omniagent.OadWorkspaces.OadWorkspace

  test "mark_ready stores the descriptor_key" do
    {:ok, ws} = OadWorkspaces.upsert(%{name: "w1", oad_base_url: "http://n1:8080", image: "img"})

    {:ok, ready} =
      OadWorkspaces.mark_ready(ws, %{
        snapshot: "w1-v1",
        revision: 1,
        descriptor_key: "descriptors/w1-v1.json"
      })

    assert ready.status == "ready"
    assert ready.snapshot == "w1-v1"
    assert ready.descriptor_key == "descriptors/w1-v1.json"
  end

  test "oad_base_url is no longer required (portable workspaces need no pin)" do
    changeset =
      OadWorkspace.changeset(%OadWorkspace{}, %{
        name: "w2",
        image: "img",
        workspace_folder: "/workspace"
      })

    assert changeset.valid?
  end
end
