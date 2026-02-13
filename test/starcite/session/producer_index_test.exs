defmodule Starcite.Session.ProducerIndexTest do
  use ExUnit.Case, async: true

  alias Starcite.Session.ProducerIndex

  test "new producer must start at sequence 1" do
    assert {:error, {:producer_seq_conflict, "p1", 1, 2}} =
             ProducerIndex.decide(%{}, "p1", 2, <<1>>, 1, 10)
  end

  test "contiguous producer sequences append and advance cursor" do
    assert {:append, index1} = ProducerIndex.decide(%{}, "p1", 1, <<1>>, 1, 10)

    assert %{"p1" => %{producer_seq: 1, session_seq: 1, hash: <<1>>}} = index1

    assert {:append, index2} = ProducerIndex.decide(index1, "p1", 2, <<2>>, 2, 10)

    assert %{"p1" => %{producer_seq: 2, session_seq: 2, hash: <<2>>}} = index2
  end

  test "same producer sequence and hash dedupes" do
    {:append, index1} = ProducerIndex.decide(%{}, "p1", 1, <<7>>, 1, 10)

    assert {:deduped, 1, ^index1} =
             ProducerIndex.decide(index1, "p1", 1, <<7>>, 2, 10)
  end

  test "same producer sequence with different hash is replay conflict" do
    {:append, index1} = ProducerIndex.decide(%{}, "p1", 1, <<7>>, 1, 10)

    assert {:error, :producer_replay_conflict} =
             ProducerIndex.decide(index1, "p1", 1, <<8>>, 2, 10)
  end

  test "non-contiguous producer sequence returns conflict with expected value" do
    {:append, index1} = ProducerIndex.decide(%{}, "p1", 1, <<1>>, 1, 10)

    assert {:error, {:producer_seq_conflict, "p1", 2, 4}} =
             ProducerIndex.decide(index1, "p1", 4, <<4>>, 2, 10)
  end

  test "lru pruning keeps most recent producer cursors" do
    {:append, index1} = ProducerIndex.decide(%{}, "p1", 1, <<1>>, 1, 2)
    {:append, index2} = ProducerIndex.decide(index1, "p2", 1, <<2>>, 2, 2)
    {:append, index3} = ProducerIndex.decide(index2, "p3", 1, <<3>>, 3, 2)

    assert Map.keys(index3) |> Enum.sort() == ["p2", "p3"]

    assert {:append, index4} = ProducerIndex.decide(index3, "p1", 1, <<4>>, 4, 2)
    assert Map.keys(index4) |> Enum.sort() == ["p1", "p3"]
  end
end
