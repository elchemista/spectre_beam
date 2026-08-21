defmodule Spectre.Beam.Socket.Codec do
  @moduledoc """
  Frame encoding for the local control socket.

  Frames are length-prefixed, so the codec only ever sees a complete payload.
  JSON is the default because the point of the socket is to be reachable from
  anything — a shell script, an editor plugin, another language. `:etf` skips
  encoding entirely for Elixir clients on the same node.
  """

  @type t :: :json | :etf

  @callback encode(map()) :: {:ok, iodata()} | {:error, term()}
  @callback decode(binary()) :: {:ok, map()} | {:error, term()}

  @json :"Elixir.Jason"

  @doc """
  Returns the codec to use, preferring JSON when an encoder is available.
  """
  @spec default() :: t()
  def default, do: if(json_available?(), do: :json, else: :etf)

  @doc "Returns true when JSON framing can be used."
  @spec json_available?() :: boolean()
  def json_available?, do: Code.ensure_loaded?(@json)

  @doc "Encodes one frame."
  @spec encode(t(), map()) :: {:ok, iodata()} | {:error, term()}
  def encode(:etf, payload), do: {:ok, :erlang.term_to_binary(payload)}

  def encode(:json, payload) do
    if json_available?() do
      # Jason is an optional host dependency, resolved at call time.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      case apply(@json, :encode, [payload]) do
        {:ok, encoded} -> {:ok, encoded}
        {:error, reason} -> {:error, {:beam_socket_encode_failed, reason}}
      end
    else
      {:error, :beam_socket_json_unavailable}
    end
  end

  @doc "Decodes one frame."
  @spec decode(t(), binary()) :: {:ok, map()} | {:error, term()}
  def decode(:etf, frame) do
    case :erlang.binary_to_term(frame, [:safe]) do
      payload when is_map(payload) -> {:ok, payload}
      other -> {:error, {:invalid_beam_socket_frame, other}}
    end
  rescue
    ArgumentError -> {:error, :invalid_beam_socket_frame}
  end

  def decode(:json, frame) do
    if json_available?() do
      # Jason is an optional host dependency, resolved at call time.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      case apply(@json, :decode, [frame]) do
        {:ok, payload} when is_map(payload) -> {:ok, payload}
        {:ok, other} -> {:error, {:invalid_beam_socket_frame, other}}
        {:error, reason} -> {:error, {:beam_socket_decode_failed, reason}}
      end
    else
      {:error, :beam_socket_json_unavailable}
    end
  end
end
