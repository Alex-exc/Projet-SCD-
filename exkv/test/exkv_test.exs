defmodule ExkvTest do
  use ExUnit.Case
  doctest Exkv

  test "greets the world" do
    assert Exkv.hello() == :world
  end
end
