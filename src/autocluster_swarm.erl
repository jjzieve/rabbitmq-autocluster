%%==============================================================================
%% @author Grzegorz Grasza <grzegorz.grasza@intel.com>
%% @copyright 2016 Intel Corporation
%% @end
%%==============================================================================
-module(autocluster_swarm).

-behavior(autocluster_backend).

%% autocluster_backend methods
-export([nodelist/0,
         lock/1,
         unlock/1,
         register/0,
         unregister/0]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-include("autocluster.hrl").


%% @spec nodelist() -> {ok, list()}|{error, Reason :: string()}
%% @doc Return a list of nodes registered in swarm
%% @end
%%
nodelist() ->
    case make_request() of
	{ok, Response} ->
	    Addresses = extract_node_list(Response),
	    {ok, lists:map(fun node_name/1, Addresses)};
	{error, Reason} ->
	    autocluster_log:info(
	      "Failed to get nodes from swarm - ~p", [Reason]),
	    {error, Reason}
    end.


-spec lock(string()) -> not_supported.
lock(_) ->
    not_supported.

-spec unlock(term()) -> ok.
unlock(_) ->
    ok.

%% @spec register() -> ok|{error, Reason :: string()}
%% @doc Stub, since this module does not update DNS
%% @end
%%
register() -> ok.


%% @spec unregister() -> ok|{error, Reason :: string()}
%% @doc Stub, since this module does not update DNS
%% @end
%%
unregister() -> ok.


%% @doc Perform a HTTP GET request to SWARM
%% @end
%%
-spec make_request() -> {ok, term()} | {error, term()}.
make_request() ->
    autocluster_httpc:get(
      autocluster_config:get(swarm_scheme),
      autocluster_config:get(swarm_host),
      autocluster_config:get(swarm_port),
      base_path(),
      [],
      [],
      [{ssl, [{cacertfile, autocluster_config:get(swarm_cert_path)}]}]).

%% @spec node_name(swarm_endpoint) -> list()  
%% @doc Return a full rabbit node name, appending hostname suffix
%% @end
%%
node_name(Address) ->
  autocluster_util:node_name(
    autocluster_util:as_string(Address) ++ autocluster_config:get(swarm_hostname_suffix)).


%% @spec maybe_ready_address(swarm_subsets()) -> list()
%% @doc Return a list of ready nodes
%% SubSet can contain also "notReadyAddresses"  
%% @end
%%
maybe_ready_address(Subset) ->
    case proplists:get_value(<<"notReadyAddresses">>, Subset) of
      undefined -> ok;
      NotReadyAddresses ->
            Formatted = string:join([binary_to_list(get_address(X))
                                     || {struct, X} <- NotReadyAddresses], ", "),
            autocluster_log:info("swarm endpoint listing returned nodes not yet ready: ~s",
                                 [Formatted])
    end,
    case proplists:get_value(<<"addresses">>, Subset) of
      undefined -> [];
      Address -> Address
    end.

%% @doc Return a list of nodes
%%    see https://docs.docker.com/engine/api/v1.37/
%% @end
%%
-spec extract_node_list({struct, term()}) -> [binary()].
extract_node_list({struct, Response}) ->
    IpLists = [[get_address(Address)
		|| {struct, Address} <- maybe_ready_address(Subset)]
	       || {struct, Subset} <- proplists:get_value(<<"subsets">>, Response)],
    sets:to_list(sets:union(lists:map(fun sets:from_list/1, IpLists))).


%% @doc Return a list of path segments that are the base path for swarm key actions
%% curl -X GET -H "Accept: application/json" --unix-socket /var/run/docker.sock 'http://localhost/networks/network_name?verbose=true'
%% Filter by Services(ServiceName) -> Tasks -> EndpointIP (this returns our "node list")
%% @end
%%
-spec base_path() -> [autocluster_httpc:path_component()].
base_path() ->
    {ok, NameSpace} = file:read_file(
			autocluster_config:get(swarm_namespace_path)),
    NameSpace1 = binary:replace(NameSpace, <<"\n">>, <<>>),
    [api, v1, namespaces, NameSpace1, endpoints,
     autocluster_config:get(swarm_service_name)].

get_address(Address) ->
    proplists:get_value(list_to_binary(autocluster_config:get(swarm_address_type)), Address).
