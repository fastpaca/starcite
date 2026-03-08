defmodule StarciteWeb.ErrorInfoTest do
  use ExUnit.Case, async: true

  alias StarciteWeb.ErrorInfo

  test "serializes expected sequence conflicts" do
    assert ErrorInfo.payload({:error, {:expected_seq_conflict, 0, 1}}) == %{
             error: "expected_seq_conflict",
             message: "Expected seq 0, current seq is 1"
           }
  end

  test "serializes producer sequence conflicts" do
    assert ErrorInfo.payload({:error, {:producer_seq_conflict, "writer-1", 3, 4}}) == %{
             error: "producer_seq_conflict",
             message: "Producer writer-1 expected seq 3, got 4"
           }
  end

  test "serializes producer replay conflicts" do
    assert ErrorInfo.payload({:error, :producer_replay_conflict}) == %{
             error: "producer_replay_conflict",
             message: "Producer sequence was already used with different event content"
           }
  end
end
