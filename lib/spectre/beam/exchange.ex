defmodule Spectre.Beam.Exchange do
  @moduledoc """
  High-level Beam result containing normalized input, Spectre turn, and receipt.
  """

  defstruct [:inbound, :input, :turn, :receipt, duplicate?: false, metadata: %{}]

  @type t :: %__MODULE__{
          inbound: Spectre.Beam.Inbound.t(),
          input: Spectre.Input.t(),
          turn: Spectre.Turn.t(),
          receipt: Spectre.Beam.Receipt.t() | nil,
          duplicate?: boolean(),
          metadata: map()
        }
end
