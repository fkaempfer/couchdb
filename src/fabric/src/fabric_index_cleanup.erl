

-module(fabric_index_cleanup).

-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-export([cleanup_index_files/1,cleanup_index_files/2,cleanup_index_files_all_nodes/2]).


-include_lib("fabric/include/fabric.hrl").



cleanup_index_files(Options) ->
    {ok, Dbs} = fabric:all_dbs(),
    [cleanup_index_files(Db,Options) || Db <- Dbs].

cleanup_index_files(DbName, Options) ->
    {ok, Db} = fabric_util:get_db(DbName, Options),
    try
        couch_db:check_is_admin(Db),
        {ok, DesignDocs} = fabric:design_docs(DbName),

        ActiveSigs = lists:map(fun(#doc{id = GroupId}) ->
            {ok, Info} = fabric:get_view_group_info(DbName, GroupId),
            binary_to_list(couch_util:get_value(signature, Info))
        end, [couch_doc:from_json_obj(DD) || DD <- DesignDocs]),

        FileList = filelib:wildcard([config:get("couchdb", "view_index_dir"),
            "/.shards/*/", couch_util:to_list(DbName), ".[0-9]*_design/mrview/*"]),

        DeleteFiles = if ActiveSigs =:= [] -> FileList; true ->
            {ok, RegExp} = re:compile([$(, string:join(ActiveSigs, "|"), $)]),
            lists:filter(fun(FilePath) ->
                re:run(FilePath, RegExp, [{capture, none}]) == nomatch
            end, FileList)
        end,
        [file:delete(File) || File <- DeleteFiles],

        rexi:reply({ok,true}),
        ok
    catch
        Error -> rexi:reply(Error)
    end.

cleanup_index_files_all_nodes(DbName, Options) ->
    Workers = lists:map(fun(Node) ->
        Ref = rexi:cast(Node, {?MODULE, cleanup_index_files, [DbName , Options]}),
        #shard{ref = Ref, node = Node}
    end,  mem3:nodes()),
    Handler = fun handle_cleanup_message/3,
    Acc0 = {Workers, length(Workers) - 1},
    Response = fabric_util:recv(Workers, #shard.ref, Handler, Acc0),
    case Response of
    {ok, _} ->

        ok;
    {timeout, {DefunctWorkers, _}} ->
        fabric_util:log_timeout(DefunctWorkers, "cleanup_index_files"),
        {error, timeout};
    Error ->
        Error
    end.

handle_cleanup_message({ok,_}, _, {_Workers, 0}) ->
    {stop, ok};
handle_cleanup_message({ok,_}, Worker, {Workers, Waiting}) ->
    {ok, {lists:delete(Worker, Workers), Waiting - 1}};
handle_cleanup_message(Error, _, _Acc) ->
    {error, Error}.
