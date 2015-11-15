-module(binary_vv).

%%-compile(export_all).

-export([new/0, increment/2, to_vv/1, from_vv/1, descends/2, dominates/2, merge/1]).

-define(INT, 32).

%% If you always use N bytes for an actor ID, and then have 1 64-bit
%% integer as the counter
new() ->
    <<>>.

increment(Actor, BVV) ->
    {Ctr, NewBVV} = case take_entry(Actor, BVV) of
                      false ->
                          {1, BVV};
                      {C, BVV1}  ->
                          {C + 1, BVV1}
                  end,
    <<Actor:24/binary, Ctr:?INT/integer, NewBVV/binary>>.

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

take_entry(_Actor, <<>>) ->
    false;
take_entry(Actor, Bin) ->
    take_entry(Bin, Actor, <<>>).

take_entry(<<>>,_Actor, _Acc) ->
    false;
take_entry(<<Actor:24/binary, Rest/binary>>, Actor, Acc)  ->
    {Counter, Rest2} =  counter(Rest),
    {Counter, <<Rest2/binary, Acc/binary>>};
take_entry(<<Other:24/binary, OtherC:?INT/integer, OtherRest/binary>>, Actor, Acc) ->
    take_entry(OtherRest, Actor, <<Other:24/binary, OtherC:?INT/integer, Acc/binary>>).

counter(<<Counter:?INT/integer, Rest/binary>>) ->
    {Counter, Rest}.

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
