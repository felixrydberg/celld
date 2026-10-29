---- MODULE RelayLiveness_TTrace_1792338333 ----
EXTENDS Sequences, TLCExt, Toolbox, RelayLiveness, Naturals, TLC, RelayLiveness_TEConstants

_expression ==
    LET RelayLiveness_TEExpression == INSTANCE RelayLiveness_TEExpression
    IN RelayLiveness_TEExpression!expression
----

_trace ==
    LET RelayLiveness_TETrace == INSTANCE RelayLiveness_TETrace
    IN RelayLiveness_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        ownGen = (2)
        /\
        recGen = ((r1 :> 1 @@ r2 :> 0))
        /\
        recExp = ((r1 :> 3 @@ r2 :> 0))
        /\
        frozen = (<<2, 0>>)
        /\
        conf = ((r1 :> <<3, 0>> @@ r2 :> <<0, 0>>))
        /\
        started = (<<TRUE, TRUE>>)
        /\
        dead = ({})
        /\
        granted = (<<3, 2>>)
        /\
        relayUp = ((r1 :> TRUE @@ r2 :> TRUE))
        /\
        now = (1)
        /\
        readFail = ((r1 :> FALSE @@ r2 :> FALSE))
        /\
        believed = (<<2, 2>>)
        /\
        ownExp = (2)
        /\
        pend = ((r1 :> <<0, 0>> @@ r2 :> <<0, 0>>))
    )
----

_init ==
    /\ now = _TETrace[1].now
    /\ believed = _TETrace[1].believed
    /\ conf = _TETrace[1].conf
    /\ started = _TETrace[1].started
    /\ readFail = _TETrace[1].readFail
    /\ ownGen = _TETrace[1].ownGen
    /\ ownExp = _TETrace[1].ownExp
    /\ frozen = _TETrace[1].frozen
    /\ relayUp = _TETrace[1].relayUp
    /\ granted = _TETrace[1].granted
    /\ pend = _TETrace[1].pend
    /\ dead = _TETrace[1].dead
    /\ recGen = _TETrace[1].recGen
    /\ recExp = _TETrace[1].recExp
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ now  = _TETrace[i].now
        /\ now' = _TETrace[j].now
        /\ believed  = _TETrace[i].believed
        /\ believed' = _TETrace[j].believed
        /\ conf  = _TETrace[i].conf
        /\ conf' = _TETrace[j].conf
        /\ started  = _TETrace[i].started
        /\ started' = _TETrace[j].started
        /\ readFail  = _TETrace[i].readFail
        /\ readFail' = _TETrace[j].readFail
        /\ ownGen  = _TETrace[i].ownGen
        /\ ownGen' = _TETrace[j].ownGen
        /\ ownExp  = _TETrace[i].ownExp
        /\ ownExp' = _TETrace[j].ownExp
        /\ frozen  = _TETrace[i].frozen
        /\ frozen' = _TETrace[j].frozen
        /\ relayUp  = _TETrace[i].relayUp
        /\ relayUp' = _TETrace[j].relayUp
        /\ granted  = _TETrace[i].granted
        /\ granted' = _TETrace[j].granted
        /\ pend  = _TETrace[i].pend
        /\ pend' = _TETrace[j].pend
        /\ dead  = _TETrace[i].dead
        /\ dead' = _TETrace[j].dead
        /\ recGen  = _TETrace[i].recGen
        /\ recGen' = _TETrace[j].recGen
        /\ recExp  = _TETrace[i].recExp
        /\ recExp' = _TETrace[j].recExp

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("RelayLiveness_TTrace_1792338333.json", _TETrace)

=============================================================================

 Note that you can extract this module `RelayLiveness_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `RelayLiveness_TEExpression.tla` file takes precedence 
  over the module `RelayLiveness_TEExpression` below).

---- MODULE RelayLiveness_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, RelayLiveness, Naturals, TLC, RelayLiveness_TEConstants

expression == 
    [
        \* To hide variables of the `RelayLiveness` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        now |-> now
        ,believed |-> believed
        ,conf |-> conf
        ,started |-> started
        ,readFail |-> readFail
        ,ownGen |-> ownGen
        ,ownExp |-> ownExp
        ,frozen |-> frozen
        ,relayUp |-> relayUp
        ,granted |-> granted
        ,pend |-> pend
        ,dead |-> dead
        ,recGen |-> recGen
        ,recExp |-> recExp
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_nowUnchanged |-> now = now'
        
        \* Format the `now` variable as Json value.
        \* ,_nowJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(now)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_nowModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].now # _TETrace[s-1].now
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE RelayLiveness_TETrace ----
\*EXTENDS IOUtils, RelayLiveness, TLC, RelayLiveness_TEConstants
\*
\*trace == IODeserialize("RelayLiveness_TTrace_1792338333.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE RelayLiveness_TETrace ----
EXTENDS RelayLiveness, TLC, RelayLiveness_TEConstants

trace == 
    <<
    ([ownGen |-> 1,recGen |-> (r1 :> 0 @@ r2 :> 0),recExp |-> (r1 :> 0 @@ r2 :> 0),frozen |-> <<0, 0>>,conf |-> (r1 :> <<0, 0>> @@ r2 :> <<0, 0>>),started |-> <<TRUE, FALSE>>,dead |-> {},granted |-> <<2, 0>>,relayUp |-> (r1 :> TRUE @@ r2 :> TRUE),now |-> 0,readFail |-> (r1 :> FALSE @@ r2 :> FALSE),believed |-> <<2, 0>>,ownExp |-> 2,pend |-> (r1 :> <<0, 0>> @@ r2 :> <<0, 0>>)]),
    ([ownGen |-> 2,recGen |-> (r1 :> 0 @@ r2 :> 0),recExp |-> (r1 :> 0 @@ r2 :> 0),frozen |-> <<2, 0>>,conf |-> (r1 :> <<0, 0>> @@ r2 :> <<0, 0>>),started |-> <<TRUE, TRUE>>,dead |-> {},granted |-> <<2, 2>>,relayUp |-> (r1 :> TRUE @@ r2 :> TRUE),now |-> 0,readFail |-> (r1 :> FALSE @@ r2 :> FALSE),believed |-> <<2, 2>>,ownExp |-> 2,pend |-> (r1 :> <<0, 0>> @@ r2 :> <<0, 0>>)]),
    ([ownGen |-> 2,recGen |-> (r1 :> 0 @@ r2 :> 0),recExp |-> (r1 :> 0 @@ r2 :> 0),frozen |-> <<2, 0>>,conf |-> (r1 :> <<0, 0>> @@ r2 :> <<0, 0>>),started |-> <<TRUE, TRUE>>,dead |-> {},granted |-> <<2, 2>>,relayUp |-> (r1 :> TRUE @@ r2 :> TRUE),now |-> 1,readFail |-> (r1 :> FALSE @@ r2 :> FALSE),believed |-> <<2, 2>>,ownExp |-> 2,pend |-> (r1 :> <<0, 0>> @@ r2 :> <<0, 0>>)]),
    ([ownGen |-> 2,recGen |-> (r1 :> 0 @@ r2 :> 0),recExp |-> (r1 :> 0 @@ r2 :> 0),frozen |-> <<2, 0>>,conf |-> (r1 :> <<0, 0>> @@ r2 :> <<0, 0>>),started |-> <<TRUE, TRUE>>,dead |-> {},granted |-> <<2, 2>>,relayUp |-> (r1 :> TRUE @@ r2 :> TRUE),now |-> 1,readFail |-> (r1 :> FALSE @@ r2 :> FALSE),believed |-> <<2, 2>>,ownExp |-> 2,pend |-> (r1 :> <<2, 0>> @@ r2 :> <<0, 0>>)]),
    ([ownGen |-> 2,recGen |-> (r1 :> 1 @@ r2 :> 0),recExp |-> (r1 :> 3 @@ r2 :> 0),frozen |-> <<2, 0>>,conf |-> (r1 :> <<3, 0>> @@ r2 :> <<0, 0>>),started |-> <<TRUE, TRUE>>,dead |-> {},granted |-> <<3, 2>>,relayUp |-> (r1 :> TRUE @@ r2 :> TRUE),now |-> 1,readFail |-> (r1 :> FALSE @@ r2 :> FALSE),believed |-> <<2, 2>>,ownExp |-> 2,pend |-> (r1 :> <<0, 0>> @@ r2 :> <<0, 0>>)])
    >>
----


=============================================================================

---- MODULE RelayLiveness_TEConstants ----
EXTENDS RelayLiveness

CONSTANTS r1, r2

=============================================================================

---- CONFIG RelayLiveness_TTrace_1792338333 ----
CONSTANTS
    Relays = { r1 , r2 }
    TTL = 2
    MaxTime = 6
    MaxGen = 2
    BrokenConfirmBeforeWrite = FALSE
    BrokenDropLiveEntry = FALSE
    BrokenUnreadableIsDead = FALSE
    BrokenVouchSuperseded = TRUE
    r2 = r2
    r1 = r1

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Sun Oct 18 17:45:33 CEST 2026