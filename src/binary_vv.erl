-module(binary_vv).

-compile(export_all).

-define(INT, 32).

%% If you always use N bytes for an actor ID, and then have 1 64-bit
%% integer as the counter
new(Size) ->
    <<Size:32/integer>>.

increment(Actor, <<Len:32/integer, Rest/binary>>) ->
    VV = increment(Len, Actor, Rest, <<>>, 0),
    <<Len:32/integer, VV/binary>>.

increment(Len, Actor, <<>>, <<>>, 0) ->
    <<Actor:Len/binary, 1:?INT/integer>>;
increment(Len, Actor, Bin, Preq, LenPreq) ->
    case Bin of
        <<>> ->
            <<Preq:LenPreq/binary, Actor:Len/binary, 1:?INT/integer>>;     
        <<Actor:Len/binary, Counter:?INT/integer, Rest/binary>> ->
            Counter2 = Counter+1,
            <<Preq:LenPreq/binary, Actor:Len/binary, Counter2:?INT/integer, Rest/binary>>;
        <<Other:Len/binary, OtherC:?INT/integer, OtherRest/binary>> ->
            increment(Len, Actor, OtherRest, <<Other:Len/binary, OtherC:?INT/integer, Preq/binary>>, LenPreq+Len+(?INT div 8))
    end.

to_vv(<<Size:32/integer, Rest/binary>>) ->
    to_vv(Size, Rest).

to_vv(_Size, <<>>) ->
    [];
to_vv(Size, Bin) ->
     <<Actor:Size/binary, Counter:?INT/integer, Rest/binary>> = Bin,
    [{Actor, Counter} | to_vv(Size, Rest)].

from_vv([{Actor1, _} | _Tail]=VV) ->
    Size = byte_size(Actor1),
    Actors = lists:foldl(fun({Actor, Cntr}, Acc) ->
                                 <<Actor:Size/binary, Cntr:?INT/integer, Acc/binary>>
                         end,
                         <<>>,
                         VV),
    <<Size:32/integer, Actors/binary>>.

descends(<<Size:32/integer, RestA/binary>>, <<Size:32/integer, RestB/binary>>) ->
    descends(Size, RestA, RestB).

descends(_Size, _, <<>>) ->
    %% all clocks descend the empty clock and themselves
    true;
descends(_Size, B, B) ->
    true;
descends(_Size, A, B) when byte_size(A) < byte_size(B) ->
    %% A cannot descend B if it is smaller than B. To descend it must
    %% cover all the same history at least, shorter means fewer actors
    false;
descends(Size, A, B) ->
    <<ActorB:Size/binary, CntrB:?INT/integer, RestB/binary>> = B,
    case take_entry(Size, ActorB, A) of
        {<<>>, 0} ->
            false;
        {RestA, CntrA} ->
            (CntrA >= CntrB) andalso descends(Size, RestA, RestB)
    end.

take_entry(_Size, _Actor, <<>>) ->
    {<<>>, 0};
take_entry(Size, Actor, Bin) ->
    case Bin of
        <<Actor:Size/binary, Counter:?INT/integer, Rest/binary>> ->
            {Rest, Counter};
        <<Other:Size/binary, OtherC:?INT/integer, OtherRest/binary>> ->
            {Rest, Counter} = take_entry(Size, Actor, OtherRest),
            {<<Other:Size/binary, OtherC:?INT/integer, Rest/binary>>, Counter}
    end.

dominates(A, B) ->
    descends(A, B) andalso not descends(B, A).

merge([<<Size:32/integer, _/binary>>=First | Clocks]) ->
    merge(Size, Clocks, First).

merge(_Size, [], Mergedest) ->
    Mergedest;
merge(Size, [Clock | Clocks], Mergedest) ->
    merge(Size, Clocks, merge(Size, Clock, Mergedest));
merge(Size, <<Size:32/integer, ClockA/binary>>,
      <<Size:32/integer, ClockB/binary>>) ->
    Merged = merge_clocks(Size, ClockA, ClockB, <<>>),
    <<Size:32/integer, Merged/binary>>.

merge_clocks(_Size, <<>>, B, Acc) ->
    <<B/binary, Acc/binary>>;
merge_clocks(_Size, A, <<>>, Acc) ->
    <<A/binary, Acc/binary>>;
merge_clocks(Size, A, B, Acc) ->
    <<ActorA:Size/binary, CounterA:?INT/integer, RestA/binary>> = A,
    {RestB, CounterB} =  take_entry(Size, ActorA, B),
    Counter = max(CounterB, CounterA),
    merge_clocks(Size, RestA, RestB,
                 <<ActorA:Size/binary, Counter:?INT/integer, Acc/binary>>).
