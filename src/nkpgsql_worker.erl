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

-module(nkpgsql_worker).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-include("nkpgsql.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").

-define(LLOG(Type, Txt, Args), lager:Type("NkPGSQL "++Txt, Args)).

-record(state, {
    srv,
    pid
}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init({SrvId, Url, Config}) ->
    process_flag(trap_exit, true),
    ResOpts = #{schemes=>#{postgresql=>postgresql, tcp=>postgresql}},
    UserOpts = maps:with([database, connect_timeout], Config),
    case nkpacket_resolve:resolve(Url, ResOpts) of
        {ok, [#nkconn{protocol=postgresql}=Conn|_]} ->
            #nkconn{ip=Ip, port=Port, opts=Opts} = Conn,
            UserOpts2 = maps:merge(Opts, UserOpts),
            PgOpts = lists:flatten([
                {host, Ip},
                {port, Port},
                {as_binary, true},
                case UserOpts2 of
                    #{user:=User} -> {user, User};
                    _ -> []
                end,
                case UserOpts2 of
                    #{password:=Pass} -> {password, Pass};
                    _ -> []
                end,
                case UserOpts2 of
                    #{database:=DB} -> {database, DB};
                    _ -> []
                end
            ]),
            ?LLOG(notice, "Worker started (~p)", [PgOpts]),
            case gen_server:start_link(pgsql_connection, PgOpts, [])  of
                {ok, Pid} ->
                    monitor(process, Pid),
                    State = #state{srv = SrvId, pid = Pid},
                    {ok, State};
                {error, {{badmatch,{{error,{pgsql_error, List}}, _}},_}} ->
                    {error, {pgsql_error, maps:from_list(List)}};
                {error, Error} ->
                    lager:warning("NKLOG PGSQL POOL CONN ERR2: ~p", [Error]),
                    {error, Error}
            end;
        _ ->
            {error, {cannot_resolve, Url}}
    end.

handle_call({query, Query, Meta}, From, #state{srv=SrvId, pid=Pid}=State) ->
    Debug = nkserver:get_cached_config(SrvId, nkpgsql, debug),
    QueryMeta = Meta#{pgsql_debug=>Debug},
    try
        Result = case is_function(Query, 1) of
            true ->
                Query(Pid);
            false ->
                %io:format("NKLOG LAUNCH QUERY: ~s\n", [Query]),
                nkpgsql:do_query(Pid, Query, QueryMeta)
        end,
        case Result of
            {ok, Data, Meta2} ->
                {reply, {ok, Data, Meta2}, State};
            Other ->
                lager:error("NKLOG PgSQL Error ~p", [Other]),
                gen_server:reply(From, {error, Other}),
                error(Other)
        end
    catch
        throw:Throw ->
            % Processing 'user' errors
            % If we are on a transaction, and some line fails,
            % it will abort but we need to rollback to be able to
            % reuse the connection
            case Query of
                #{auto_rollback:=true} ->
                    case catch nkpgsql:do_query(Pid, <<"ROLLBACK;">>, #{pgsql_debug=>Debug}) of
                        {ok, _, _} ->
                            ok;
                        no_transaction ->
                            ok;
                        Error ->
                            ?LLOG(notice, "error performing Rollback: ~p", [Error]),
                            error(rollback_error)
                    end;
                _ ->
                    ok
            end,
            {reply, {error, Throw}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    lager:error("NKLOG STOP: ~p", [State]),
    {stop, normal, State};

handle_cast(error, State) ->
    lager:error("NKLOG ERROR: ~p", [State]),
    error(forced);

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, #state{pid=Pid}=State) ->
    ?LLOG(warning, "PgSQL has fallen, stopping", []),
    {stop, pgsql_conn_stop, State};

handle_info(Info, State) ->
    ?LLOG(warning, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{pid=Pid}) ->
    pgsql_connection:close({pgsql_connection, Pid}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


