%%%-------------------------------------------------------------------
%%% @author forgottenEntity
%%% @doc
%%%
%%% @end
%%% Created : 12. Feb 2019 19:22
%%%-------------------------------------------------------------------
-module(yarc_index_descriptor).
-author("forgottenEntity").

%% API
-export([new/1,
         get_index_record_count/1
]).

-include("../include/yarc_index.hrl").


-define(store, <<"store">>).
-define(index_record_count, <<"index_record_count">>).
-define(max_index_record_size, <<"max_index_record_size">>).

%%====================================================================
%% API functions
%%====================================================================

new(Store) ->
  #{?store => Store, ?index_record_count => ?default_index_size, ?max_index_record_size => ?default_max_index_record_size}.


get_index_record_count(IndexDescriptorMap) ->
  maps:get(?index_record_count, IndexDescriptorMap).

%%====================================================================
%% Internal functions
%%====================================================================
