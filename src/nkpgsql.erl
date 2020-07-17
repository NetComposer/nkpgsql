%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(nkpgsql).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([query/2, query/3, do_query/3]).
-export([get_connection/1, release_connection/2, stop_connection/1]).
-export([pool_status/1]).
-export([t1/0, t2/0]).
-define(LLOG(Type, Txt, Args), lager:Type("NkPGSQL "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================

-type query_fun() :: fun((pid()) -> {ok, list(), Meta::map()}).

-type query_meta() :: #{
        auto_roll_back => boolean(),            % Default false
        result_fun => fun(([op()], Meta::map()) -> term()),
        term() => term()
    }.

-type op() ::
    {Op::term(), list(), Meta::map()}.


-type pgsql_error() ::
    uniqueness_violation |
    foreign_key_violation |
    no_transaction |
    field_unknown |
    syntax_error |
    data_value_invalid |
    pgsql_full_error().


-type pgsql_full_error() ::
    #{
        code => binary(),
        message => binary(),
        severity => binary()
    }.


%% ===================================================================
%% API
%% ===================================================================


%%t() ->
%%    t(1),
%%    lager:error("NKLOG NEXT"),
%%    t2(3),
%%    t3(20000),
%%    t2(20).
%%
%%t(N) when N < 10000 ->
%%    N2 = nklib_util:lpad(N, 5, $0),
%%    Q = <<"SELECT ", N2/binary, ", * From actors">>,
%%    spawn_link(
%%        fun() -> {ok, _, _} = query(netcomp_rcp_actor_pgsql_srv, Q, #{}) end),
%%    t(N+1);
%%
%%t(_) ->
%%    ok.
%%
%%t2(N) when N > 0 ->
%%    lager:error("NKLOG STATUS ~p", [pool_status(netcomp_rcp_actor_pgsql_srv)]),
%%    timer:sleep(5000),
%%    t2(N-1);
%%
%%t2(_) ->
%%    ok.
%%
%%t3(N) when N < 30000 ->
%%    N2 = nklib_util:lpad(N, 5, $0),
%%    Q = <<"SELECT ", N2/binary, ", * From actors">>,
%%    spawn_link(
%%        fun() -> {ok, _, _} = query(netcomp_rcp_actor_pgsql_srv, Q, #{}) end),
%%    t3(N+1);
%%
%%t3(_) ->
%%    ok.


t1() ->
    Q = <<"
        BEGIN;
        SELECT abc;
        COMMIT;
    ">>,
    query(netcomp_rcp_actor_pgsql_srv, Q, #{auto_rollback => true}).


t2() ->
    Q = <<"
        SELECT 1;
    ">>,
    query(netcomp_rcp_actor_pgsql_srv, Q, #{auto_rollback => true}).



%% @doc Performs a query
-spec query(nkserver:id(), binary()|query_fun()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, pgsql_error()}|term()}.

query(SrvId, Query) ->
    query(SrvId, Query, #{}).


%% @doc Performs a query
-spec query(nkserver:id(), binary()|query_fun(), query_meta()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, pgsql_error()}|term()}.

query(SrvId, Query, QueryMeta) ->
    query(SrvId, Query, QueryMeta, 2).


query(SrvId, Query, QueryMeta, Tries) ->
    Name = nkpgsql_plugin:get_pool_name(SrvId),
    Fun = fun(Worker) ->
        gen_server:call(Worker, {query, Query, QueryMeta}, infinity)
    end,
    % Timeout is for checkout
    case poolboy:transaction(Name, Fun, infinity) of
        {ok, Data, Meta} ->
            {ok, Data, Meta};
        {error, Error} when Tries >= 1 ->
            ?LLOG(warning, "PgSQL transaction error: ~p, retrying", [Error]),
            timer:sleep(100),
            query(SrvId, Query, QueryMeta, Tries-1);
        {error, Error} ->
            ?LLOG(warning, "PgSQL transaction error: ~p, no more retrying", [Error]),
            {error, Error}
    end.


%%query(SrvId, Query, QueryMeta) ->
%%    Debug = nkserver:get_cached_config(SrvId, nkpgsql, debug),
%%    QueryMeta2 = QueryMeta#{pgsql_debug=>Debug},
%%    case get_connection(SrvId) of
%%        {ok, Pid} ->
%%            try
%%                {ok, Data, MetaData}= case is_function(Query, 1) of
%%                    true ->
%%                        Query(Pid);
%%                    false ->
%%                        do_query(Pid, Query, QueryMeta2)
%%                end,
%%                {ok, Data, MetaData}
%%            catch
%%                throw:Throw ->
%%                    % Processing 'user' errors
%%                    % If we are on a transaction, and some line fails,
%%                    % it will abort but we need to rollback to be able to
%%                    % reuse the connection
%%                    case QueryMeta2 of
%%                        #{auto_rollback:=true} ->
%%                            case catch do_query(Pid, <<"ROLLBACK;">>, #{pgsql_debug=>Debug}) of
%%                                {ok, _, _} ->
%%                                    ok;
%%                                no_transaction ->
%%                                    ok;
%%                                Error ->
%%                                    ?LLOG(notice, "error performing Rollback: ~p", [Error]),
%%                                    error(rollback_error)
%%                            end;
%%                        _ ->
%%                            ok
%%                    end,SELEC
%%                    {error, Throw};
%%                Class:CError:Trace ->
%%                    % For other errors, we better close the connection
%%                    ?LLOG(warning, "error in query: ~p, ~p, ~p", [Class, CError, Trace]),
%%                    nkpgsql:stop_connection(Pid),
%%                    {error, internal_error}
%%            after
%%                release_connection(SrvId, Pid)
%%            end;
%%        {error, Error} ->
%%            {error, Error}
%%    end.


%% @private
do_query(Pid, Query, QueryMeta) when is_pid(Pid) ->
    case maps:get(pgsql_debug, QueryMeta, false) of
        true ->
            %?LLOG(debug, "PreQuery: ~s\n~p", [Query, QueryMeta]),
            ok;
        _ ->
            ok
    end,
    %?LLOG(info, "PreQuery: ~s", [Query]),
    case do_simple_query(Pid, Query) of
        {ok, Ops, PgMeta} ->
            case maps:get(pgsql_debug, QueryMeta, false) of
                true ->
                    ?LLOG(debug, "PostQuery: ~s\n~p", [Query, PgMeta]),
                    ok;
                _ ->
                    ok
            end,
            % lager:error("NKLOG OPS ~p", [Ops]),
            case QueryMeta of
                #{result_fun:=ResultFun} ->
                    ResultFun(Ops, QueryMeta#{pgsql=>PgMeta});
                _ ->
                    List = [Rows || {_Op, Rows, _OpMeta} <- Ops],
                    {ok, List, QueryMeta#{pgsql=>PgMeta}}
            end;
        {error, {pgsql_error, #{routine:=<<"NewUniquenessConstraintViolationError">>}}} ->
            throw(uniqueness_violation);
        {error, {pgsql_error, #{code := <<"23503">>}}} ->
            throw(foreign_key_violation);
        {error, {pgsql_error, #{code := <<"23505">>}}} ->
            throw(duplicated_name);
        {error, {pgsql_error, #{code := <<"XX000">>}}=Error} ->
            ?LLOG(info, "no_transaction PGSQL error: ~p\n~s", [Error, list_to_binary([Query])]),
            throw(no_transaction);
        {error, {pgsql_error, #{code := <<"42P01">>}}} ->
            throw(field_unknown);
        {error, {pgsql_error, #{code := <<"42601">>}}=Error} ->
            ?LLOG(warning, "syntax PGSQL error: ~p\n~s", [Error, list_to_binary([Query])]),
            throw(syntax_error);
        {error, {pgsql_error, #{code := <<"22023">>}}} ->
            throw(data_value_invalid);
        {error, {pgsql_error, Error}} ->
            ?LLOG(warning, "unknown PGSQL error: ~p\n~s", [Error, list_to_binary([Query])]),
            throw(Error);
        {error, Error} ->
            throw(Error)
    end.



%% @private
-spec do_simple_query(pid(), binary()) ->
    {ok, [op()], Meta::map()} |
    {error, {pgsql_error, pgsql_error()}|term()}.

do_simple_query(Pid, Query) ->
    Start = nklib_date:epoch(msecs),
    % Opts = [{return_descriptions, true}],
    Opts = [],
    case pgsql_connection:simple_query(Query, Opts, {pgsql_connection, Pid}) of
        {error, {pgsql_error, List}} ->
            {error, {pgsql_error, maps:from_list(List)}};
        {error, Error} ->
            lager:error("PGSQL UNEXPECTED ERR ~p", [Error]),
            {error, Error};
        % If it is a transaction, first error that happens will abort, and it
        % will appear first in the list of errors (no more error can be on the list)
        % If it is not a transaction, the result will be error only if the last
        % sentence is an error
        [{error, {pgsql_error, List}}|_] ->
            % lager:error("NKLOG ERROR OTHER ~p", [Rest]),
            {error, {pgsql_error, maps:from_list(List)}};
        [{error, Error}|_] ->
            lager:error("PGSQL UNEXPECTED ERR2 ~p", [Error]),
            {error, Error};
        Data ->
            Time = nklib_date:epoch(msecs) - Start,
            {ok, parse_results(Data, []), #{time=>Time}}
    end.


%% @private
%%parse_results([], [Acc]) ->
%%    [Acc];

parse_results([], Acc) ->
    Acc;

parse_results([{Op, Desc, List}|Rest], Acc) ->
    Desc2 = [{N, F} || {_, N, _, _, _, _, _, F} <- Desc],
    parse_results(Rest, [{Op, List, #{fields=>Desc2}}|Acc]);

parse_results([{Op, List}|Rest], Acc) ->
    parse_results(Rest, [{Op, List, #{}}|Acc]);

parse_results(Other, Acc) ->
    parse_results([Other], Acc).






%% @doc
get_connection(SrvId) ->
    get_connection(SrvId, 1000).


%% @private
get_connection(SrvId, Tries) when Tries > 0 ->
    case nkpacket_pool:get_exclusive_pid(SrvId) of
        {ok, Pid, _Meta} ->
            {ok, Pid};
        {error, max_connections_reached} ->
            lager:warning("Max connections reached (~p). ~p retries left", [SrvId, Tries]),
            timer:sleep(50),
            get_connection(SrvId, Tries-1);
        {error, Error} ->
            lager:warning("Error in get_connection (~p): ~p", [SrvId, Error]),
            {error, Error}
    end;

get_connection(_SrvId, _Tries) ->
    lager:warning("Error in get_connection (~p), no more tries"),
    {error, max_connections_reached}.



%% @doc
release_connection(SrvId, Pid) ->
    nkpacket_pool:release_exclusive_pid(SrvId, Pid).


%% @doc
stop_connection(Pid) ->
    nkpgsql_plugin:conn_stop(Pid).

pool_status(SrvId) ->
    Name = nkpgsql_plugin:get_pool_name(SrvId),
    Status = poolboy:status(Name),
    Queue = element(4, sys:get_state(Name)),
    {Status, queue:len(Queue)}.



%%%% ===================================================================
%%%% Luerl API
%%%% ===================================================================
%%
%%%% @doc
%%luerl_query(SrvId, PackageId, [Query]) ->
%%    case query(SrvId, PackageId, Query) of
%%        {ok, List, _Meta} ->
%%            [parse_rows(List)];
%%        {error, {pgsql_error, Error}} ->
%%            [nil, pgsql_error, Error];
%%        {error, Error} ->
%%            {error, Error}
%%    end.
%%
%%
%%parse_rows(Rows) ->
%%    lists:map(
%%        fun(Row) ->
%%            lists:map(
%%                fun
%%                    ({{_Y, _M, _D}=Date, {H, M, S1}}) ->
%%                        S2 = round(S1 * 1000),
%%                        Secs = S2 div 1000,
%%                        Msecs = S2 - (Secs * 1000),
%%                        Unix1 = nklib_util:gmt_to_timestamp({Date, {H, M, Secs}}),
%%                        Unix2 = Unix1 * 1000 + Msecs,
%%                        Unix2;
%%                    (Term) when is_binary(Term); is_integer(Term); is_float(Term) ->
%%                        Term;
%%                    (Other) ->
%%                        nklib_util:to_binary(Other)
%%                end,
%%                tuple_to_list(Row))
%%        end,
%%        Rows).
%%
%%
%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).