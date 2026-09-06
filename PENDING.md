# Waiting on the next release

This branch holds examples that are finished and verified, but that need a fix
which is on `main` in [acemq-go-amqp](https://github.com/AceMQ-Company/acemq-go-amqp)
and not yet in a released module version.

The examples on `main` resolve released versions on purpose — an example that
needs an unreleased fix would be a red build for everyone who clones the
repository, so it waits here instead.

| Example | Needs |
|---|---|
| `basic/04-replay` | a selective replay reading the whole queue rather than stopping at the first message its filter declines |

Verified against a broker with the library built from source: three invoices
dead-lettered, two replayed for one tenant, the third replayed afterwards.

When a release carries the fix, merge this branch into `main`, add the row to
the README table, and delete this file.
