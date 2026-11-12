# 048 · After a big enterprise customer went live, one web server ran at six times its share and timed out while the other seven idled

## Problem
A SaaS web app runs 8 identical backends behind a load balancer. The app
keeps some per-user state in backend memory (an in-process cache and upload
progress), so the balancer used **source-IP hash affinity**: hash the
client's IP address, pick a backend, and every request from that IP lands on
the same server.

Then a large enterprise customer rolled the product out to **3,000
employees**. All of them reach the internet through **one corporate NAT**, so
the balancer sees a single public IP for all 3,000. Every one of their
requests went to the same backend. That server's latency climbed until its
health checks failed; when it was pulled from the pool, the whole enterprise
moved together to the next server and took that one down too. Meanwhile the
other backends were nearly idle and the enterprise's employees reported the
app as "down".

In the simulation (3,000 users behind one NAT IP plus 1,000 users on their
own IPs, 10 requests each, 40,000 requests, seed 48), source-IP affinity sent
**31,300 requests to `backend-2`** against a fair share of **5,000** (6.3x),
while the other seven got only **1,050-1,400** each.

## Root cause
**The affinity key was too coarse.** A source IP is not a user: NAT, corporate
proxies, carrier-grade NAT on mobile networks and VPNs put thousands of users
behind one address. Hashing a key that thousands of users share can only pick
one backend for all of them, however good the hash is. (Compare problem 046,
where balancers herded on stale load data, and problem 034, where one tenant
was too heavy for consistent hashing without load bounds; here the hashing is
fine and the input to it is wrong.)

## Solution
`SessionAffinityBalancer` keys affinity on a **per-user session cookie**:

1. If the request carries a `lb_session` cookie, hash it onto a
   **consistent-hash ring** (`HashRing`, 200 virtual nodes per backend) and
   send it to that backend. Every user behind the NAT has their own cookie,
   so they spread over all backends but each stays on one.
2. **Fallback:** a request with no (or an empty) cookie gets a freshly
   generated random session id, is routed by it, and the id is returned to be
   set as a cookie. Its next requests stick to that backend. Clients that
   never keep cookies (some API clients, bots) get a new id each time, so they
   are spread evenly rather than pinned by IP.
3. The ring means a backend leaving or joining moves only its own share of
   sessions. Removing one of 8 backends moved **12.2%** of 10,000 sessions on
   the ring, versus **87.8%** with plain `hash % backend_count`.

Per-backend requests, same traffic:

| Routing | Busiest backend | Quietest backend | Fair share |
|---|---|---|---|
| Source-IP hash (seed 48) | **31,300** | 1,050 | 5,000 |
| Session cookie on ring (seed 48) | 5,480 | 4,690 | 5,000 |
| Session cookie on ring (seeds 0-4) | 5,240-5,790 | 4,440-4,670 | 5,000 |

## Trade-offs
- **Cookie affinity needs a layer-7 balancer** that terminates TLS and reads
  HTTP headers. A pure layer-4 (TCP) balancer only sees IPs and ports; there
  the options are hashing on the full 5-tuple (no stickiness across
  connections) or moving state out of the backends.
- **The client chooses its cookie.** A malicious client can pick values that
  land on one backend, but only with its own traffic; rate limiting per
  session/IP still applies. Signing the cookie stops forged ids if that
  matters.
- **Affinity is still per user, not per load.** One very heavy user (or many
  heavy sessions on one backend) can still overload it; bounded-load
  consistent hashing (problem 034) or spilling to a second choice handles
  that.
- **Best alternative: no affinity at all.** Keeping session state in a shared
  store (Redis, database) lets any backend serve any request and makes plain
  least-requests or two-random-choices balancing possible. Affinity is then
  only a cache-locality optimisation.
- Cookieless clients lose stickiness; if they need it, use a stable
  application-level key they do send (API key, user id in a token).

## Run
```bash
python -m unittest -v test_solution.py
```
