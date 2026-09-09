# advanced/04 — encrypting payloads

Bodies the broker cannot read, a key rotated without an outage, and what one
altered byte does.

## What it shows

- **`crypto.Wrap` around any codec.** The payload is encoded as usual and the
  bytes are then encrypted.
- **A keyring with two keys**, and the order a rotation has to happen in.
- **The key identifier read off a message without decrypting it.**
- **A tampered body parked, not retried**, with a reason.
- **A missing key named**, fatally.

## Running it

```bash
docker compose up -d
go run ./advanced/04-encrypting-payloads
```

## What to look for

```
keyring holds [2026-01 2026-04], writing with 2026-01
rotated: new messages are written with 2026-04

A-7  key=2026-01   type=order.placed   body=ae0107323032362d303172593acc0d85…
A-8  key=2026-04   type=order.placed   body=ae0107323032362d3034b70ef52acd54…

published the same message with one byte of its tag flipped
parked 99 bytes, because: could not be decoded: acemq: this message did not decrypt with key "2026-01"; it was altered, or encrypted with a different key of the same name

a consumer holding only 2026-01, reading A-8: acemq: this message was encrypted with key "2026-04", which is not on this keyring (it holds 2026-01). Retrying will not help; add the key or dead-letter the message
```

**Read the two `body=` prefixes.** `ae 01` is the magic byte and the format
version. `07` is the length of the key identifier, and the next seven bytes are
`2026-01` and `2026-04` in ASCII — the key name travels in the clear, because a
consumer has to know which key to try before it can decrypt anything. It names a
key; it does not reveal one.

Everything after that is a nonce and ciphertext. The example asserts that no
customer name and not even the string `orderId` appears in the body, checked
against the bytes as they arrived rather than against the decoded payload.

**`type=order.placed` is readable**, and that is the warning rather than a bug.

## The framing is the family's

```
[0xAE][1 byte version][1 byte key id length][key id][12 byte nonce][ciphertext+tag]
```

Byte for byte what Java, .NET, Python and Ruby write. A message encrypted by this
example opens in any of them given the same key, and theirs open here. **0.5.0 is
the release where that became true of Go**: up to v0.3.0 this library wrote a
framing of its own, with a two-byte big-endian length and no magic byte, which no
other library could read. Those bodies are still *read* — so a queue filled before
the change can be drained by an upgraded consumer — and are never written again.
Reading them goes away in v0.6.0.

The magic byte is why the two cannot be confused, and why a consumer pointed at a
plaintext queue is told *this message was not written by crypto.Codec* rather
than reporting a decryption failure for a message that was never encrypted.

## Rotation, in the order that works

1. **Add the new key to every keyring** — every consumer, every producer — and
   deploy that. Nothing has changed on the wire yet; every process can now read
   messages written with either key.
2. **Then** make it current somewhere, with `keyring.Use("2026-04")`, and new
   messages start carrying the new name.
3. When nothing written with the old key is left anywhere — including the
   dead-letter queues, which is the step people forget — drop it.

Doing 2 before 1 produces messages nothing can open. `NewKeyring` takes the
writing key first and `Add` deliberately does *not* make a key current, which is
the shape of step 1.

A keyring with one key cannot rotate at all: there is no moment at which both the
messages already in the queue and the ones about to be written can be read.

## What one changed byte does

AES-GCM authenticates as well as encrypts, so an altered body does not decrypt
into something slightly different — it does not decrypt. The header is
authenticated but not encrypted, which is why a key identifier changed in flight
also makes the message fail to open rather than open as something else.

The message is **parked**, not retried: the error is fatal, because the same bytes
fail the same way every time. Parked rather than dead-lettered because a body
nothing could read is a producer's problem and a handler that failed five times
is usually the world's, and whoever drains those queues should not have to sort
them by hand.

## What this does not protect

**Headers travel in the clear.** The envelope — identity, type, correlation,
causation — is how the library routes and retries, so it cannot be encrypted
without the broker losing the ability to do its job. Do not put anything secret
in a header.

**It does not authenticate the sender.** Anybody holding the key can write a
message this codec will happily decrypt. A key is a shared secret between
everyone who may publish and everyone who may read, and nothing more. If you need
to know *who* sent a message, sign it.

**It does not hide the traffic.** Sizes, timings, routing keys and queue depths
are all still visible, and for many systems that is enough to infer a great deal.

## The keys in this example are not keys

They are drawn with `crypto.NewKey` at start-up, which means the second run of
this example cannot read the first run's messages. That is fine here and is not a
deployment: real keys come from somewhere that can hand the same bytes to several
processes and take them back — a secret manager, a mounted file, a KMS — and
never from source control.

`crypto.Key.String()` never prints the secret, and `Keyring.IDs()` lists names
only, so both are safe in a log line or a health endpoint.

A secret that came from a passphrase needs a key derivation function — PBKDF2,
scrypt, Argon2 — before it is a key. The library refuses a short one rather than
padding or hashing it into shape, because both would make a weak key look strong.
