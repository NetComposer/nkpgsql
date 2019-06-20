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

%% @doc Default plugin callbacks
-module(nkpgsql_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([pgsql_init/1]).
-export([srv_timed_check/2]).
-include("nkpgsql.hrl").

-define(CHECK_TIME_SECS, 300).


%% ===================================================================
%% Offered callbacks
%% ===================================================================

%% @doc Called on plugin initialization
%% Can be used to set a db schema
-spec pgsql_init(nkserver:id()) ->
    ok | {error, nkserver:msg()}.

pgsql_init(_SrvId) ->
    ok.


%% ===================================================================
%% Implemented callbacks
%% ===================================================================

%% Create database:
%%  psql -h 127.0.0.1 -p 26257 -U root -d system -c "CREATE DATABASE netcomp_sample;"


srv_timed_check(#{id:=SrvId}=Srv, #{nkpgsql_last_check:=Last}=State) ->
    Now = nklib_date:epoch(secs),
    case Now - Last > ?CHECK_TIME_SECS of
        true ->
            case nkpgsql_util:check_db(SrvId) of
                ok ->
                    ok;
                {error, Error} ->
                    lager:error("NkPGSQL: Cannot connect to database (~p)", [Error])
            end,
            {continue, [Srv, State#{nkpgsql_last_check=>Now}]};
        false ->
            continue
    end;

srv_timed_check(Srv, State) ->
    Now = nklib_date:epoch(secs),
    {continue, [Srv, State#{nkpgsql_last_check=>Now}]}.
