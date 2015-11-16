-module(vv_bm).

-compile(export_all).

increment() ->
    Actors = [crypto:rand_bytes(24) || _ <- lists:seq(1, 50)],
    Events = 100000,
    increment(Actors, Events, binary_vv:new(), term_to_binary(riak_dt_vclock:fresh()), {[], []}).

increment(_Actors, 0, _BVV, _VC, Times) ->
    stats(Times);
increment(Actors, Events, BVV, VC, Times) ->
    Actor = random_actor(Actors),
    {BVVTime, NewBVV} = timer:tc(binary_vv, increment, [Actor, BVV]),
    F = fun() -> VC0 = binary_to_term(VC), VC1 = riak_dt_vclock:increment(Actor, VC0), term_to_binary(VC1) end,
    {VCTime, NewVC} = timer:tc(F),%%riak_dt_vclock, increment, [Actor, VC]),
    increment(Actors, Events - 1, NewBVV, NewVC, update_times(BVVTime, VCTime, Times)).
    
    
random_actor([Actor]) ->
    Actor;
random_actor(Actors) ->
    N = crypto:rand_uniform(1, length(Actors)),
    lists:nth(N, Actors).

update_times(BVVTime, VCTime, {BVVTimes, VCTimes}) ->
    {[BVVTime | BVVTimes], [VCTime | VCTimes]}.

stats({BVVTimes, VCTimes}) ->
    {
      {bvv, stats(BVVTimes)},
      {vc, stats(VCTimes)}
    };
stats(Times) ->
    Min = lists:min(Times),
    Max = lists:max(Times),
    Avg = average(Times),
    Sorted = lists:sort(Times),
    Percentiles = [percentile(Sorted, P) || P <- [0.25, 0.75, 0.90, 0.99]],
    [{quantiles, Percentiles},
     {min, Min},
     {max, Max},
     {avg, Avg}, 
     {raw, Sorted}].

percentile(Sorted, P) ->
    Index = P * length(Sorted),
    percentile(trunc(Index), ceil(Index), Sorted).

percentile(Index, Index, Sorted) ->
    %% Whole number, av of nth + nth+1
    average([lists:nth(Index, Sorted) , lists:nth(Index+1, Sorted)]);
percentile(_, Index, Sorted) ->
    lists:nth(Index, Sorted).

average(L) ->
    Sum = lists:sum(L),
    Sum / length(L).
    
ceil(X) ->
    T = erlang:trunc(X),
        case (X - T) of
        Neg when Neg < 0 ->
                T;
            Pos when Pos > 0 -> T + 1;
            _ -> T
    end.
