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

%% @doc Default callbacks for plugin definitions
-module(nkpgsql_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_deps/0, plugin_config/3, plugin_cache/3,
         plugin_start/3, plugin_update/4, plugin_stop/3]).
%%-export([connect_link/2]).
%%-export([conn_resolve/3, conn_start/1, conn_stop/1]).
-export([get_pool_name/1]).

-include("nkpgsql.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").

%% ===================================================================
%% Default implementation
%% ===================================================================


plugin_deps() ->
	[nkserver].


%% @doc
plugin_config(_SrvId, Config, #{class:=nkpgsql}) ->
    Syntax = #{
        targets => {list, #{
            url => binary,
            weight => {integer, 1, 1000},
            pool => {integer, 1, 1000},
            max_exclusive => {integer, 0, 1000},
            '__mandatory' => [url]
        }},
        flavour => {atom, [postgresql, cockroachdb, yugabyte]},
        database => binary,
        debug => boolean,
        resolve_interval_secs => {integer, 0, none},
        '__mandatory' => [targets],
        '__defaults' => #{
            flavour => postgresql
        }
    },
    nkserver_util:parse_config(Config, Syntax).


plugin_cache(_SrvId, Config, _Service) ->
    Cache = #{
        flavour => maps:get(flavour, Config),
        debug => maps:get(debug, Config, [])
    },
    {ok, Cache}.


%% @doc
plugin_start(SrvId, Config, Service) ->
    case insert2(SrvId, Config, Service) of
        ok ->
%%            case insert(SrvId, Config, Service) of
%%                ok ->
                    timer:sleep(1000),
                    case wait_for_db(SrvId, 10) of
                        ok ->
                            ?CALL_SRV(SrvId, pgsql_init, [SrvId]);
                        {error, Error} ->
                            {error, Error}
                    end;
%%                {error, Error} ->
%%                    {error, Error}
%%            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc
plugin_stop(SrvId, _Config, _Service) ->
    nkserver_workers_sup:remove_all_childs(SrvId).


%% @doc
plugin_update(SrvId, NewConfig, OldConfig, Service) ->
    case NewConfig of
        OldConfig ->
            ok;
        _ ->
            insert2(SrvId, NewConfig, Service)
    end.



%% ===================================================================
%% Internal
%% ===================================================================


insert2(SrvId, #{targets:=[#{url:=Url, pool:=Size}=T]}=Config, Service) ->
    Max = maps:get(max_exclusive, T, 0),
    Name = nklib_util:to_atom(list_to_binary([
        nklib_util:to_binary(SrvId), $-,
        nklib_util:to_binary(?MODULE), $-,
        <<"poolboy">>])),
    Id = {SrvId, ?MODULE, poolboy},
    persistent_term:put(Id, Name),
    PoolArgs = [
        {name, {local, Name}},
        {worker_module, nkpgsql_worker},
        {size, Size},
        {max_overflow, Max},
        {strategy, fifo}
    ],
    WorkerArgs = {SrvId, Url, Config},
    Spec = #{
        id => Id,
        start =>  {poolboy, start_link, [PoolArgs, WorkerArgs]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [poolboy]
    },
    case nkserver_workers_sup:update_child2(SrvId, Spec, #{}) of
        {ok, Op, _Pid} ->
            ?SRV_LOG(notice, "Pooler started: (~p)", [PoolArgs], Service),
            ?SRV_LOG(info, "pooler2 updated (~p)", [Op], Service),
            ok;
        {error, Error} ->
            ?SRV_LOG(notice, "pooler2 start/update error: ~p", [Error], Service),
            {error, Error}
    end.


get_pool_name(SrvId) ->
    Id = {SrvId, ?MODULE, poolboy},
    persistent_term:get(Id).


%%
%%%% @private
%%insert(SrvId, Config, Service) ->
%%    PoolConfig = Config#{
%%        targets => maps:get(targets, Config),
%%        debug => maps:get(debug, Config, false),
%%        resolve_interval_secs => maps:get(resolve_interval_secs, Config, 0),
%%        conn_resolve_fun => fun ?MODULE:conn_resolve/3,
%%        conn_start_fun => fun ?MODULE:conn_start/1,
%%        conn_stop_fun => fun ?MODULE:conn_stop/1
%%    },
%%    Spec = #{
%%        id => SrvId,
%%        start => {nkpacket_pool, start_link, [SrvId, PoolConfig]}
%%    },
%%    case nkserver_workers_sup:update_child2(SrvId, Spec, #{}) of
%%        {ok, Op, _Pid} ->
%%            ?SRV_LOG(info, "pooler updated (~p)", [Op], Service),
%%            ok;
%%        {error, Error} ->
%%            ?SRV_LOG(notice, "pooler start/update error: ~p", [Error], Service),
%%            {error, Error}
%%    end.
%%
%%
%%
%%%% @private
%%conn_resolve(#{url:=Url}, Config, _Pid) ->
%%    ResOpts = #{schemes=>#{postgresql=>postgresql, tcp=>postgresql}},
%%    UserOpts = maps:with([database, connect_timeout], Config),
%%    case nkpacket_resolve:resolve(Url, ResOpts) of
%%        {ok, List1} ->
%%            do_conn_resolve(List1, UserOpts, []);
%%        {error, Error} ->
%%            {error, Error}
%%    end.
%%
%%%% @private
%%do_conn_resolve([], _UserOpts, Acc) ->
%%    {ok, lists:reverse(Acc), #{}};
%%
%%do_conn_resolve([Conn|Rest], UserOpts, Acc) ->
%%    case Conn of
%%        #nkconn{protocol=postgresql, transp=Transp, opts=Opts} ->
%%            Transp2 = case Transp of
%%                tcp ->
%%                    tcp;
%%                undefined ->
%%                    tcp
%%            end,
%%            Opts2 = maps:merge(Opts, UserOpts),
%%            Conn2 = Conn#nkconn{transp=Transp2, opts=Opts2},
%%            do_conn_resolve(Rest, UserOpts, [Conn2|Acc]);
%%        _ ->
%%            {error, invalid_protocol}
%%    end.
%%
%%
%%%% @private
%%conn_start(#nkconn{transp=_Transp, ip=Ip, port=Port, opts=Opts}) ->
%%    PgOpts = lists:flatten([
%%        {host, Ip},
%%        {port, Port},
%%        {as_binary, true},
%%        case Opts of
%%            #{user:=User} -> {user, User};
%%            _ -> []
%%        end,
%%        case Opts of
%%            #{password:=Pass} -> {password, Pass};
%%            _ -> []
%%        end,
%%        case Opts of
%%            #{database:=DB} -> {database, DB};
%%            _ -> []
%%        end
%%    ]),
%%    case gen_server:start(pgsql_connection, PgOpts, [])  of
%%        {ok, Pid} ->
%%            {ok, Pid};
%%        {error, {{badmatch,{{error,{pgsql_error, List}}, _}},_}} ->
%%            {error, {pgsql_error, maps:from_list(List)}};
%%        {error, Error} ->
%%            lager:warning("NKLOG PGSQL POOL CONN ERR2: ~p", [Error]),
%%            {error, Error}
%%    end.
%%
%%
%%%% @private
%%conn_stop(Pid) ->
%%    pgsql_connection:close({pgsql_connection, Pid}).
%%
%%
%%connect_link(Url, Opts) ->
%%     case nkpacket:connect(Url, Opts) of
%%         {ok, #nkport{pid=Pid}} ->
%%             link(Pid),
%%             {ok, Pid};
%%         {error, Error} ->
%%             {error, Error}
%%     end.


%% @private
wait_for_db(SrvId, Tries) when Tries > 0 ->
    case nkpgsql_util:check_db(SrvId) of
        ok ->
            ok;
        {error, Error } ->
            lager:notice("NkPGSQL: Error connecting to database (~p): ~p tries left", [Error, Tries]),
            timer:sleep(1000),
            wait_for_db(SrvId, Tries-1)
    end;

wait_for_db(_SrvId, _Tries) ->
    {error, database_connection}.
