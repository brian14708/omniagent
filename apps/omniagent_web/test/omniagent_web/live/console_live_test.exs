defmodule OmniagentWeb.ConsoleLiveTest do
  use ExUnit.Case, async: true

  alias OmniagentWeb.ConsoleLive

  describe "parse_diff/1" do
    test "splits a unified diff into per-file blocks with +/- counts" do
      diff = """
      diff --git a/lib/foo.ex b/lib/foo.ex
      index 111..222 100644
      --- a/lib/foo.ex
      +++ b/lib/foo.ex
      @@ -1,3 +1,3 @@
       defmodule Foo do
      -  def old, do: 1
      +  def new, do: 2
       end
      diff --git a/lib/bar.ex b/lib/bar.ex
      index 333..444 100644
      --- a/lib/bar.ex
      +++ b/lib/bar.ex
      @@ -0,0 +1,1 @@
      +added line
      """

      assert [foo, bar] = ConsoleLive.parse_diff(diff)

      assert foo.path == "lib/foo.ex"
      assert foo.added == 1
      assert foo.removed == 1
      # +++/--- header lines are not counted as add/remove.
      assert Enum.count(foo.lines, &(&1.type == :add)) == 1
      assert Enum.count(foo.lines, &(&1.type == :del)) == 1
      assert Enum.any?(foo.lines, &(&1.type == :hunk))

      assert bar.path == "lib/bar.ex"
      assert bar.added == 1
      assert bar.removed == 0
    end

    test "returns [] for empty or nil input" do
      assert ConsoleLive.parse_diff("") == []
      assert ConsoleLive.parse_diff(nil) == []
    end
  end
end
