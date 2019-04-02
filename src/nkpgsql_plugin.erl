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
-export([connect_link/2]).
-export([conn_resolve/3, conn_start/1, conn_stop/1]).

-include("nkpgsql.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").

%% ===================================================================
%% Default implementation
%% ===================================================================


plugin_deps() ->
	[nkserver].


%% @doc
plugin_config(_SrvId, Config, #{class:=?PACKAGE_CLASS_PGSQL}) ->
    Syntax = #{
        targets => {list, #{
            url => binary,
            weight => {integer, 1, 1000},
            pool => {integer, 1, 1000},
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
    insert(SrvId, Config, Service).


plugin_stop(SrvId, _Config, _Service) ->
    nkserver_workers_sup:remove_all_childs(SrvId).


%% @doc
plugin_update(SrvId, NewConfig, OldConfig, Service) ->
    case NewConfig of
        OldConfig ->
            ok;
        _ ->
            insert(SrvId, NewConfig, Service)
    end.



%% ===================================================================
%% Internal
%% ===================================================================



%% @private
insert(SrvId, Config, Service) ->
    PoolConfig = Config#{
        targets => maps:get(targets, Config),
        debug => maps:get(debug, Config, false),
        resolve_interval_secs => maps:get(resolve_interval_secs, Config, 0),
        conn_resolve_fun => fun ?MODULE:conn_resolve/3,
        conn_start_fun => fun ?MODULE:conn_start/1,
        conn_stop_fun => fun ?MODULE:conn_stop/1
    },
    Spec = #{
        id => SrvId,
        start => {nkpacket_pool, start_link, [SrvId, PoolConfig]}
    },
    case nkserver_workers_sup:update_child(SrvId, Spec, #{}) of
        {added, _} ->
            ?SRV_LOG(info, "pooler started", [], Service),
            ok;
        upgraded ->
            ?SRV_LOG(info, "pooler upgraded", [], Service),
            ok;
        not_updated ->
            ?SRV_LOG(debug, "pooler didn't upgrade", [], Service),
            ok;
        {error, Error} ->
            ?SRV_LOG(notice, "pooler start/update error: ~p", [Error], Service),
            {error, Error}
    end.



%% @private
conn_resolve(#{url:=Url}, Config, _Pid) ->
    ResOpts = #{schemes=>#{postgresql=>postgresql, tcp=>postgresql}},
    UserOpts = maps:with([database, connect_timeout], Config),
    case nkpacket_resolve:resolve(Url, ResOpts) of
        {ok, List1} ->
            do_conn_resolve(List1, UserOpts, []);
        {error, Error} ->
            {error, Error}
    end.

%% @private
do_conn_resolve([], _UserOpts, Acc) ->
    {ok, lists:reverse(Acc), #{}};

do_conn_resolve([Conn|Rest], UserOpts, Acc) ->
    case Conn of
        #nkconn{protocol=postgresql, transp=Transp, opts=Opts} ->
            Transp2 = case Transp of
                tcp ->
                    tcp;
                undefined ->
                    tcp
            end,
            Opts2 = maps:merge(Opts, UserOpts),
            Conn2 = Conn#nkconn{transp=Transp2, opts=Opts2},
            do_conn_resolve(Rest, UserOpts, [Conn2|Acc]);
        _ ->
            {error, invalid_protocol}
    end.


%% @private
conn_start(#nkconn{transp=_Transp, ip=Ip, port=Port, opts=Opts}) ->
    PgOpts = lists:flatten([
        {host, Ip},
        {port, Port},
        {as_binary, true},
        case Opts of
            #{user:=User} -> {user, User};
            _ -> []
        end,
        case Opts of
            #{password:=Pass} -> {password, Pass};
            _ -> []
        end,
        case Opts of
            #{database:=DB} -> {database, DB};
            _ -> []
        end
    ]),
    case gen_server:start(pgsql_connection, PgOpts, [])  of
        {ok, Pid} ->
            {ok, Pid};
        {error, {{badmatch,{{error,{pgsql_error, List}}, _}},_}} ->
            {error, {pgsql_error, maps:from_list(List)}};
        {error, Error} ->
            lager:warning("NKLOG PGSQL POOL CONN ERR2: ~p", [Error]),
            {error, Error}
    end.


%% @private
conn_stop(Pid) ->
    pgsql_connection:close({pgsql_connection, Pid}).


connect_link(Url, Opts) ->
     case nkpacket:connect(Url, Opts) of
         {ok, #nkport{pid=Pid}} ->
             link(Pid),
             {ok, Pid};
         {error, Error} ->
             {error, Error}
     end.


