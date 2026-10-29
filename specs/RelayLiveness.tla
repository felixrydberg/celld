--------------------------- MODULE RelayLiveness ---------------------------
(***************************************************************************)
(* Relay-vouched node liveness (todo/aggregate-node-liveness.md).           *)
(*                                                                         *)
(* One node name, its process generations (a same-name restart replaces   *)
(* the running process at once, as `CELLD_NODE` is a singleton key), and   *)
(* the relays it heartbeats to. A relay writes one bucket record holding   *)
(* the node's deadline and confirms that deadline to the node only once   *)
(* the write has landed. The node's own record keeps its generation and a  *)
(* fallback deadline it can renew itself. A peer takes the node's cells   *)
(* over when every record it must read says the deadline has passed.       *)
(*                                                                         *)
(* Clocks are perfect, as in the coordination specs: one `now`.            *)
(*                                                                         *)
(* Write safety (one writer per epoch, no lost acknowledged write) does   *)
(* not rest on this protocol and is not modelled. What rests on it is    *)
(* read freshness: a process must not serve cells a peer took over.       *)
(*                                                                         *)
(* The BROKEN_* constants each switch one rule off; every one of them      *)
(* must produce a counterexample.                                          *)
(***************************************************************************)
EXTENDS Naturals, TLC

CONSTANTS
    Relays,          \* the relays the node heartbeats to (its `via` list)
    TTL,             \* lease lifetime
    MaxTime,         \* model bound on `now`
    MaxGen,          \* model bound on same-name restarts
    BrokenConfirmBeforeWrite,  \* R1 off: confirm a heartbeat before writing it
    BrokenDropLiveEntry,       \* R2 off: drop an entry whose deadline is ahead
    BrokenUnreadableIsDead,    \* R4 off: an unreadable record counts as absent
    BrokenVouchSuperseded      \* R3 off: vouch for any generation that asks

Gens == 1..MaxGen
Times == 0..(MaxTime + TTL)
None == 0      \* no heartbeat pending / nothing to confirm / no entry (Times start at 0, deadlines at TTL)

Max2(a, b) == IF a >= b THEN a ELSE b

VARIABLES
    now,         \* the one clock
    ownGen,      \* generation in the node's own record
    ownExp,      \* fallback deadline in the node's own record
    started,     \* [Gens -> BOOLEAN]: that process exists
    believed,    \* [Gens -> Times]: the deadline the process acts on
    recGen,      \* [Relays -> Gens \cup {0}]: generation in the relay's record (0: no entry)
    recExp,      \* [Relays -> Times]: deadline in the relay's record
    pend,        \* [Relays -> [Gens -> Times \cup {None}]]: latest heartbeat not yet written, as time + 1
    conf,        \* [Relays -> [Gens -> Times]]: written deadline not yet confirmed (None: nothing)
    relayUp,     \* [Relays -> BOOLEAN]
    readFail,    \* [Relays -> BOOLEAN]: a peer's read of this record fails now
    dead,        \* generations whose cells peers took over
    granted,     \* [Gens -> Times]: latest deadline ever made durable for that generation
    frozen       \* [Gens -> Times]: `granted` when that generation was superseded

vars == <<now, ownGen, ownExp, started, believed, recGen, recExp, pend, conf,
          relayUp, readFail, dead, granted, frozen>>

-----------------------------------------------------------------------------
(* What peers can see. *)

Vouches(r) == recGen[r] = ownGen

\* The node's deadline across every record that vouches for its current
\* generation.
Published ==
    LET relayed == {recExp[r] : r \in {q \in Relays : Vouches(q)}}
    IN  IF relayed = {} THEN ownExp
        ELSE Max2(ownExp, CHOOSE m \in relayed : \A x \in relayed : x <= m)

\* The same, over only the records a peer managed to read.
SeenPublished ==
    LET relayed == {recExp[r] : r \in {q \in Relays : Vouches(q) /\ ~readFail[q]}}
    IN  IF relayed = {} THEN ownExp
        ELSE Max2(ownExp, CHOOSE m \in relayed : \A x \in relayed : x <= m)

\* R4: a record that fails to read leaves the node undecided, never dead.
DecideDead ==
    IF BrokenUnreadableIsDead
    THEN SeenPublished <= now
    ELSE (\A r \in Relays : ~readFail[r]) /\ Published <= now

Serving(g) == started[g] /\ believed[g] > now

-----------------------------------------------------------------------------
Init ==
    /\ now = 0
    /\ ownGen = 1
    /\ ownExp = TTL
    /\ started = [g \in Gens |-> g = 1]
    /\ believed = [g \in Gens |-> IF g = 1 THEN TTL ELSE 0]
    /\ recGen = [r \in Relays |-> 0]
    /\ recExp = [r \in Relays |-> 0]
    /\ pend = [r \in Relays |-> [g \in Gens |-> None]]
    /\ conf = [r \in Relays |-> [g \in Gens |-> None]]
    /\ relayUp = [r \in Relays |-> TRUE]
    /\ readFail = [r \in Relays |-> FALSE]
    /\ dead = {}
    /\ granted = [g \in Gens |-> IF g = 1 THEN TTL ELSE 0]
    /\ frozen = [g \in Gens |-> 0]

\* Time passes. A process past its deadline has fenced itself for good:
\* nothing below extends a deadline that is not ahead of `now`.
Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<ownGen, ownExp, started, believed, recGen, recExp, pend,
                   conf, relayUp, readFail, dead, granted, frozen>>

\* A serving process heartbeats a live relay. Only the latest one matters.
Heartbeat(g, r) ==
    /\ Serving(g)
    /\ relayUp[r]
    /\ pend' = [pend EXCEPT ![r][g] = now + 1]
    /\ UNCHANGED <<now, ownGen, ownExp, started, believed, recGen, recExp,
                   conf, relayUp, readFail, dead, granted, frozen>>

\* A relay writes its record for one generation's pending heartbeat. R3: only
\* the generation in the node's own record. R2: never lower the deadline of
\* the generation already recorded. The written deadline becomes confirmable.
RelayWrite(r, g) ==
    /\ relayUp[r]
    /\ pend[r][g] /= None
    /\ IF g = ownGen \/ BrokenVouchSuperseded
       THEN LET exp == (pend[r][g] - 1) + TTL
                kept == IF recGen[r] = g THEN Max2(recExp[r], exp) ELSE exp
            IN  /\ recGen' = [recGen EXCEPT ![r] = g]
                /\ recExp' = [recExp EXCEPT ![r] = kept]
                /\ conf' = [conf EXCEPT ![r][g] = Max2(@, kept)]
                /\ granted' = [granted EXCEPT ![g] = Max2(@, kept)]
       ELSE UNCHANGED <<recGen, recExp, conf, granted>>
    /\ pend' = [pend EXCEPT ![r][g] = None]
    /\ UNCHANGED <<now, ownGen, ownExp, started, believed, relayUp, readFail,
                   dead, frozen>>

\* R1: a relay confirms only a deadline it has written. The confirmation can
\* also be lost (ConfirmLost), and a process past its deadline ignores it.
Confirm(r, g) ==
    /\ conf[r][g] /= None
    /\ Serving(g)
    /\ believed' = [believed EXCEPT ![g] = Max2(@, conf[r][g])]
    /\ conf' = [conf EXCEPT ![r][g] = None]
    /\ UNCHANGED <<now, ownGen, ownExp, started, recGen, recExp, pend,
                   relayUp, readFail, dead, granted, frozen>>

ConfirmLost(r, g) ==
    /\ conf[r][g] /= None
    /\ conf' = [conf EXCEPT ![r][g] = None]
    /\ UNCHANGED <<now, ownGen, ownExp, started, believed, recGen, recExp,
                   pend, relayUp, readFail, dead, granted, frozen>>

\* Broken R1: the relay answers the heartbeat before writing anything.
ConfirmEarly(r, g) ==
    /\ BrokenConfirmBeforeWrite
    /\ pend[r][g] /= None
    /\ Serving(g)
    /\ believed' = [believed EXCEPT ![g] = Max2(@, (pend[r][g] - 1) + TTL)]
    /\ UNCHANGED <<now, ownGen, ownExp, started, recGen, recExp, pend, conf,
                   relayUp, readFail, dead, granted, frozen>>

\* R3's early half: a relay tells a superseded process so, and it fences.
Superseded(g) ==
    /\ Serving(g)
    /\ g /= ownGen
    /\ believed' = [believed EXCEPT ![g] = 0]
    /\ UNCHANGED <<now, ownGen, ownExp, started, recGen, recExp, pend, conf,
                   relayUp, readFail, dead, granted, frozen>>

\* R5: the fallback. The process CASes its own record (which checks the
\* generation) and acts on the new deadline once the write has landed.
SelfRenew(g) ==
    /\ Serving(g)
    /\ g = ownGen
    /\ ownExp' = Max2(ownExp, now + TTL)
    /\ believed' = [believed EXCEPT ![g] = Max2(@, now + TTL)]
    /\ granted' = [granted EXCEPT ![g] = Max2(@, now + TTL)]
    /\ UNCHANGED <<now, ownGen, started, recGen, recExp, pend, conf, relayUp,
                   readFail, dead, frozen>>

\* R2: an entry leaves a record only once its deadline has passed.
DropEntry(r) ==
    /\ recGen[r] /= 0
    /\ BrokenDropLiveEntry \/ recExp[r] <= now
    /\ recGen' = [recGen EXCEPT ![r] = 0]
    /\ recExp' = [recExp EXCEPT ![r] = 0]
    /\ UNCHANGED <<now, ownGen, ownExp, started, believed, pend, conf,
                   relayUp, readFail, dead, granted, frozen>>

\* A relay stops; its record stays in the bucket, its memory does not.
RelayCrash(r) ==
    /\ relayUp[r]
    /\ relayUp' = [relayUp EXCEPT ![r] = FALSE]
    /\ pend' = [pend EXCEPT ![r] = [g \in Gens |-> None]]
    /\ conf' = [conf EXCEPT ![r] = [g \in Gens |-> None]]
    /\ UNCHANGED <<now, ownGen, ownExp, started, believed, recGen, recExp,
                   readFail, dead, granted, frozen>>

RelayRecover(r) ==
    /\ ~relayUp[r]
    /\ relayUp' = [relayUp EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<now, ownGen, ownExp, started, believed, recGen, recExp,
                   pend, conf, readFail, dead, granted, frozen>>

\* A peer's reads of one record start or stop failing.
ReadFlip(r) ==
    /\ readFail' = [readFail EXCEPT ![r] = ~@]
    /\ UNCHANGED <<now, ownGen, ownExp, started, believed, recGen, recExp,
                   pend, conf, relayUp, dead, granted, frozen>>

\* A peer takes the node's cells over. They belong to the node name, so every
\* process started so far loses them; a later restart acquires cells afresh.
Takeover ==
    /\ ownGen \notin dead
    /\ DecideDead
    /\ dead' = dead \cup {g \in Gens : started[g]}
    /\ UNCHANGED <<now, ownGen, ownExp, started, believed, recGen, recExp,
                   pend, conf, relayUp, readFail, granted, frozen>>

\* A same-name restart replaces the running process at once.
Restart ==
    /\ ownGen < MaxGen
    /\ LET g == ownGen + 1
       IN  /\ ownGen' = g
           /\ ownExp' = now + TTL
           /\ started' = [started EXCEPT ![g] = TRUE]
           /\ believed' = [believed EXCEPT ![g] = now + TTL]
           /\ granted' = [granted EXCEPT ![g] = now + TTL]
           /\ frozen' = [frozen EXCEPT ![ownGen] = granted[ownGen]]
    /\ UNCHANGED <<now, recGen, recExp, pend, conf, relayUp, readFail, dead>>

Next ==
    \/ Tick
    \/ Takeover
    \/ Restart
    \/ \E g \in Gens : Superseded(g) \/ SelfRenew(g)
    \/ \E r \in Relays :
          \/ DropEntry(r) \/ RelayCrash(r) \/ RelayRecover(r) \/ ReadFlip(r)
          \/ \E g \in Gens :
                \/ Heartbeat(g, r) \/ RelayWrite(r, g) \/ Confirm(r, g)
                \/ ConfirmLost(r, g) \/ ConfirmEarly(r, g)

Spec == Init /\ [][Next]_vars

\* The relays are interchangeable, so the checker may treat their
\* permutations as one state.
RelaySymmetry == Permutations(Relays)

-----------------------------------------------------------------------------
(* Invariants. *)

TypeOK ==
    /\ now \in 0..MaxTime
    /\ ownGen \in Gens
    /\ ownExp \in Times
    /\ started \in [Gens -> BOOLEAN]
    /\ believed \in [Gens -> Times]
    /\ recGen \in [Relays -> Gens \cup {0}]
    /\ recExp \in [Relays -> Times]
    /\ pend \in [Relays -> [Gens -> 0..(MaxTime + 1)]]
    /\ conf \in [Relays -> [Gens -> Times]]
    /\ dead \subseteq Gens

\* Read freshness: no process serves cells a peer took over.
NoStaleServe == \A g \in Gens : Serving(g) => g \notin dead

\* A superseded generation is never granted more than it held when it was
\* replaced: relays cannot keep an old process alive.
SupersededNotExtended == \A g \in Gens : g < ownGen => granted[g] <= frozen[g]

\* Canaries, each expected to FAIL in the passing configuration, so the
\* checked invariants are not vacuous: a relay confirmation does carry the
\* process past its own record's deadline, and takeovers do happen.
CanaryNoRelayExtension == \A g \in Gens : g = ownGen => believed[g] <= ownExp
CanaryNoTakeover == dead = {}

=============================================================================
