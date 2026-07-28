# Spectre Beam

`spectre_beam` is the external-channel boundary package for Spectre. Its
version 0.1.2 integration owns the Stack-local `channel/2` DSL and compiles an
immutable description of configured adapters.

## Installation

The project is distributed from GitHub:

```elixir
def deps do
  [
    {:spectre_beam, github: "elchemista/spectre_beam"}
  ]
end
```

## Stack

```elixir
defmodule MyApp.AI do
  use Spectre.Stack

  install Spectre.Beam do
    channel :telegram, MyApp.Telegram
    channel :whatsapp, MyApp.WhatsApp
  end
end
```

Installation describes the boundary but does not start channel adapters or
authorize them for an Agent.
