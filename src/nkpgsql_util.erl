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

-module(nkpgsql_util).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([quote/1]).

%% ===================================================================
%% Public
%% ===================================================================



%% @private
quote(Field) when is_binary(Field) -> <<$', (to_field(Field))/binary, $'>>;
quote(Field) when is_list(Field) -> <<$', (to_field(Field))/binary, $'>>;
quote(Field) when is_integer(Field); is_float(Field) -> to_bin(Field);
quote(true) -> <<"TRUE">>;
quote(false) -> <<"FALSE">>;
quote(null) -> <<"NULL">>;
quote(Field) when is_atom(Field) -> quote(atom_to_binary(Field, utf8));
quote(Field) when is_map(Field) ->
    case nklib_json:encode(Field) of
        error ->
            lager:error("Error enconding JSON: ~p", [Field]),
            error(json_encode_error);
        Json when is_binary(Json)->
            quote(Json)
    end.


to_field(Field) ->
    Field2 = to_bin(Field),
    case binary:match(Field2, <<$'>>) of
        nomatch ->
            Field2;
        _ ->
            re:replace(Field2, <<$'>>, <<$',$'>>, [global, {return, binary}])
    end.

%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).




