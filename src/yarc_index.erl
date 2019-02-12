%%%-------------------------------------------------------------------
%%% @author forgottenEntity
%%% @doc
%%%
%%% @end
%%% Created : 12. Feb 2019 17:41
%%%-------------------------------------------------------------------
-module(yarc_index).
-author("forgottenEntity").

%% API
-export([get_descriptor/1,
  put_descriptor/2,
  fetch_index_segments/2,
  get_index/1
  ]).

-include("../include/yarc_index.hrl").

%%====================================================================
%% API functions
%%====================================================================

add_entry(IndexName, Key, IndexEntry) ->
  ok.

delete_entry(IndexName, Key) ->
  ok.

get_index(IndexName) ->
  IndexDescriptor = get_descriptor(IndexName),
  fetch_index_segments(IndexName, IndexDescriptor).


%%====================================================================
%% Internal functions
%%====================================================================

get_descriptor(IndexName) ->
  RiakConnection = yarc_riak_connection_pool:get_connection(yarc_riak_pool),
  IndexDescriptorMap = case yarc_riak_connection:get(RiakConnection, ?yarc_index_descriptor_bucket, IndexName, ?DEFAULT_TIMEOUT) of
             {ok, RiakObj} ->
               jsx:decode(riakc_obj:get_value(RiakObj), [return_maps]);
             Error ->
               Error
           end,
  yarc_riak_connection_pool:return_connection(yarc_riak_pool, RiakConnection),
  IndexDescriptorMap.

put_descriptor(IndexName, IndexDescriptorMap) ->
  JSon = jsx:encode(IndexDescriptorMap),
  RiakConnection = yarc_riak_connection_pool:get_connection(yarc_riak_pool),
  RiakObj = riakc_obj:new(?yarc_index_descriptor_bucket, IndexName, JSon),
  yarc_riak_connection:put(RiakConnection, ?DEFAULT_TIMEOUT, RiakObj),
  yarc_riak_connection_pool:return_connection(yarc_riak_pool, RiakConnection),
  ok.

fetch_index_segments(IndexName, IndexDescriptor) ->
  IndexRecordCount = yarc_index_descriptor:get_index_record_count(IndexDescriptor),
  fetch_index_segments(IndexName, IndexDescriptor, IndexRecordCount, 0, []).


fetch_index_segments(_IndexName, _IndexDescriptor, IndexRecordCount, IndexRecordCount, IndexData) ->
  IndexData;
fetch_index_segments(IndexName, IndexDescriptor, IndexRecordCount, IndexSegmentOrdinal, IndexData) ->
  SegmentData = fetch_index_segment(IndexName, IndexSegmentOrdinal),
  fetch_index_segments(IndexName, IndexDescriptor, IndexRecordCount, IndexSegmentOrdinal + 1, [SegmentData | IndexData]).

fetch_index_segment(IndexName, IndexSegmentOrdinal) ->
  Ordinal = integer_to_binary(IndexSegmentOrdinal),
  IndexSegmentName = <<IndexName/binary,"_",Ordinal/binary>>,
  RiakConnection = yarc_riak_connection_pool:get_connection(yarc_riak_pool),
  Object = case yarc_riak_connection:get(RiakConnection, IndexName, IndexSegmentName, ?DEFAULT_TIMEOUT) of
             {ok, RiakObj} ->
               riakc_obj:get_value(RiakObj);
             Error ->
               Error
           end,
  yarc_riak_connection_pool:return_connection(yarc_riak_pool, RiakConnection),
  Object.