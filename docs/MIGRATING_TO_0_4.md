# Migrate Beam integration from 0.3 to 0.4

This guide covers integration with Spectre `~> 0.4.0`. Install the compatible
package versions shown in the [README](../README.md), then follow the ownership
and callback changes below. Satellite versions remain independent of core.

## Move ownership, preserve capabilities

| Earlier integration | AgentServer 0.4 |
| --- | --- |
| `Beam.ask`, Chat/Gateway or Turn handler as entry point | Host resolves an existing Server; `enqueue/3` accepts normalized inbound; consumer receives replies |
| Route body stages old Beam action/effect DSL | Agent declares route; Server callback returns generated `beam` command |
| Provider send wired directly to a route | Bind `deliver` to a host GAM operation; admitted executor uses stable command identity |
| Legacy `Spectre.Turn` return used to send replies | Do not synthesize a Turn; wire the Server consumer and governed external delivery explicitly |

1. Keep the Stack installation and supported provider/configuration choices.
   Change any dependency pins to the compatible package versions or reviewed source refs.
2. Keep routes/classification declarations in the Agent, set `server: MyApp.Server`,
   and move execution callbacks to `use Spectre.Server, tools: MyApp.AI`.
3. Return the context containing generated helper commands. Handle actual tagged
   results in `handle_result/3`; there is no synchronous helper result and no import
   of invented <code>Spectre.Beam.beam/3</code> functions.
4. For consequential work, configure AgentServer allowed operations and consent
   continuations where required. Installing a package does not authorize end-user
   effects. Follow the [GAM manifest](https://spectre.guide/en/governed-act-model).
5. Choose and supervise the actual runtime resources and checkpoint Store. Keep
   handles outside durable assigns; use portable result/state representations.
6. Exercise recovery with your application's real scope, storage and provider
   contracts. A changed checkpoint fingerprint requires an explicit core migration;
   old Instance snapshots are not automatically AgentServer snapshots.

## What remains available

Channel adapters, content, pipelines and native delivery remain useful independently. The `ask`, Chat, Gateway, `Spectre.turn` and Instance examples in the retained guide describe the retained legacy runtime surface; they do not automatically create a Spectre 0.4 AgentServer.

See the [retained reference](CHANNELS_AND_LEGACY_RUNTIME.md) for these APIs and
[0.4 helper/lifecycle contracts](SPECTRE_0_4.md) for the new integration. Migration
is an ownership/API change, not a promise that every legacy convenience now
implicitly routes through AgentServer or GAM.

## Skill Servers and the shared GAM

A mounted Skill may have a separate Server, state and checkpoint. It automatically
reuses the root AgentServer's GAM Domain, authority ledger and execution boundary;
no Skill Host, second Domain or manual ingress is part of this migration. Keep
root operation permissions and mount `governed:` references aligned. Proposal
provenance remains recorded, confirmations stay bound to the exact Skill and
presentation, and timer closure cannot erase a Duty. Parallel workers remain
subject to the same kernel admission laws.

See [the package integration guide](SPECTRE_0_4.md#server-lifecycle-and-skills) and the
[core Skill ownership and consent contract](https://github.com/elchemista/spectre/blob/0.4.0/docs/SKILLS.md).
