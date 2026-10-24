# Intake: profile a described project

In design mode the brief is the codebase. Read all of it first (the user's message, a brief file, an RFC, a diagram), then fill the profile from `RECON.md`'s format with what the brief says.

## Ask only for what's missing

Most findings depend on scale and consistency needs, and briefs often leave these out. Ask about the gaps in **one message**, at most five questions, taken in this order of importance:

1. **Load**: peak requests per second, or daily active users, now and in a year.
2. **Consistency**: which operations must never be lost or doubled (payments, stock, bookings), and which can lag by seconds.
3. **Data volume and shape**: rows or events per day, hot entities, read/write ratio.
4. **Team and deployment**: how many engineers and teams, and how often each part ships.
5. **Availability**: the uptime target, and which flows must keep working when a dependency is down.

Skip any question the brief already answers. If the user declines, doesn't know, or nobody can answer, fill each remaining gap with the most conservative reading of the brief. For consistency, treat money, bookings and stock as must-never-be-lost-or-doubled. With no numbers at all, assume one team, about 100 requests per second at peak and 99.9% availability. Record every such choice under `assumptions` in the profile, and grade severities against it. Turn the targets into peak rates per flow (for example, GPS writes per second) under `derived load`; the lenses grade against those numbers.

## Gaps are signals

What the brief **leaves out** is often a finding, and lens agents should treat it as one. Examples:
- a payment flow with no word on what happens when the provider times out;
- events with no delivery guarantee;
- "real-time" with no fan-out mechanism.

Leave the profile's `deployables` entries as planned components, marked `(planned)`.

Done when the profile is filled, each field from the brief, an answer, or a stated assumption.
