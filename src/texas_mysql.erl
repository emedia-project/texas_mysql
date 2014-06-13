-module(texas_mysql).

-export([start/0]).
-export([connect/6, exec/2, close/1]).
-export([create_table/2]).
-export([insert/3, select/4, update/4, delete/3]).

-define(STRING_SEPARATOR, $").
-define(STRING_QUOTE, $\\).

-type connection() :: any().
-type err() :: any().
-type tablename() :: atom().
-type data() :: any().
-type clause_type() :: where | group | order | limit.
-type clause() :: {clause_type(), string(), [tuple()]} |
                  {clause_type(), string(), []}.
-type clauses() :: [clause()] | [].

-spec start() -> ok.
start() ->
  crypto:start(),
  application:start(emysql),
  ok.

-spec connect(string(), string(), string(), integer(), string(), any()) -> 
  {ok, connection()} | {error, err()}.
connect(User, Password, Server, Port, Database, Options) ->
  lager:debug("Open database ~p", [Database]),
  Pool = list_to_atom("epool_"++Database++"_"++Server),
  try
    case emysql:add_pool(Pool, [{user, User},
                                {password, Password},
                                {host, Server},
                                {port, list_to_integer(Port)},
                                {database, Database}] ++ Options) of
      ok -> {ok, Pool};
      _ -> {error, connection_failed}
    end
  catch
    _:_ -> {error, connection_failed}
  end.

-spec close(connection()) -> ok | error.
close(Conn) ->
  try 
    emysql:remove_pool(Conn)
  catch
    _:_ -> error
  end.

-spec create_table(connection(), tablename()) -> ok | error.
create_table(Conn, Table) ->
  SQLCmd = sql(
    create_table, 
    atom_to_list(Table), 
    lists:map(fun(Field) ->
            sql(
              column_def, 
              atom_to_list(Field),
              Table:type(Field),
              Table:len(Field),
              Table:autoincrement(Field),
              Table:not_null(Field),
              Table:unique(Field),
              Table:default(Field))
        end, Table:fields())),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of 
    {ok_packet, _, _, _, _, _, _} -> ok;
    _ -> error
  end.

-spec insert(connection(), tablename(), data()) -> data() | {error, err()}.
insert(Conn, Table, Record) ->
  {Fields, Values} = lists:foldl(fun(Field, {FieldsAcc, ValuesAcc}) ->
          case Record:Field() of
            undefined -> {FieldsAcc, ValuesAcc};
            Value -> {FieldsAcc ++ ["`"++atom_to_list(Field)++"`"], 
                      ValuesAcc ++ [texas_sql:to_sql_string(Value, ?STRING_SEPARATOR, ?STRING_QUOTE)]}
          end
      end, {[], []}, Table:fields()),
  SQLCmd = sql(insert, atom_to_list(Table), Fields, Values),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok_packet, _, _, ID, _, _, _} ->
      case Table:table_pk_id() of
        {none, null} -> Record;
        {ok, Col} -> select(Conn, Table, first, 
                            [{where, io_lib:format("`~p` = :id", [Col]), [{id, ID}]}])
      end;
    {error_packet, _, _, _, E} -> {error, E}
  end.

-spec select(connection(), tablename(), first | all, clauses()) -> 
  data() | [data()] | [] | {error, err()}.
select(Conn, Table, Type, Clauses) -> 
  SQLCmd = sql(select, atom_to_list(Table), sql(clause, Clauses ++ case Type of
          first -> [{limit, "1", []}];
          _ -> []
        end)),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {result_packet, _, _, [], _} -> [];
    {result_packet, _, _, _, _} = Result -> 
      case Type of
        first -> 
          [Data|_] = emysql:as_json(Result),
          Table:new(assoc(Table, Data));
        _ ->
          lists:map(fun(Data) ->
                Table:new(assoc(Table, Data))
            end, emysql:as_json(Result))
      end;
    {error_packet, _, _, _, E} -> {error, E}
  end.

-spec update(connection(), tablename(), data(), [tuple()]) -> [data()] | {error, err()}.
update(Conn, Table, Record, UpdateData) ->
  Where = join(lists:foldl(fun(Field, W) ->
            case Record:Field() of
              undefined -> W;
              Value -> W ++ [{Field, Value}]
            end
        end, [], Table:fields()), " AND "),
  Set = join(UpdateData, ", "),
  SQLCmd = "UPDATE `" ++ atom_to_list(Table) ++ "` SET " ++ Set ++ " WHERE " ++ Where ++ ";",
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok_packet, _, _, _, _, _, _} ->
      UpdateRecord = lists:foldl(fun({Field, Value}, Rec) ->
              Rec:Field(Value)
          end, Record, UpdateData),
      select(Conn, Table, all, [texas_sql:record_to_where_clause(Table, UpdateRecord, "`")]);
    {error_packet, _, _, _, E} -> {error, E}
  end.

-spec delete(connection(), tablename(), data()) -> ok | {error, err()}.
delete(Conn, Table, Record) ->
  WhereClause = texas_sql:record_to_where_clause(Table, Record, "`"),
  SQLCmd = sql(delete, atom_to_list(Table), sql(clause, [WhereClause])),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok_packet, _, _, _, _, _, _} -> ok;
    {error_packet, _, _, _, E} -> {error, E}
  end.

% Private --

exec(SQL, Conn) ->
  emysql:execute(Conn, list_to_binary(SQL)).

assoc(Table, Datas) ->
  lists:map(fun({Col, Value}) ->
        ACol = binary_to_atom(Col, utf8),
        case Value of
          null -> {ACol, undefined};
          _ -> {ACol, texas_type:to(Table:type(ACol), Value)}
        end
    end, Datas).

join(KVList, Sep) ->
  string:join(lists:map(fun({K, V}) ->
          io_lib:format("`~p` = ~s", [K, texas_sql:to_sql_string(V, ?STRING_SEPARATOR, ?STRING_QUOTE)])
      end, KVList), Sep).

sql(create_table, Name, ColDefs) -> 
  "CREATE TABLE IF NOT EXISTS `" ++ Name ++ "` (" ++ string:join(ColDefs, ", ") ++ ");";
sql(select, Name, Clauses) ->
  "SELECT * FROM `" ++ Name ++ "` " ++ string:join(Clauses, " ") ++ ";";
sql(delete, Name, Clauses) ->
  "DELETE FROM `" ++ Name ++ "` " ++ string:join(Clauses, " ") ++ ";";
sql(type, id, {ok, Len}) -> " INTEGER(" ++ integer_to_list(Len) ++ ")";
sql(type, id, _) -> " INTEGER";
sql(type, integer, {ok, Len}) -> " INTEGER(" ++ integer_to_list(Len) ++ ")";
sql(type, integer, _) -> " INTEGER";
sql(type, string, {ok, Len}) -> " VARCHAR(" ++ integer_to_list(Len) ++ ")";
sql(type, string, _) -> " TEXT";
sql(type, float, _) -> " REAL";
sql(type, _, _) -> " TEXT".
sql(insert, Table, Fields, Values) ->
  "INSERT INTO `" ++ Table ++ "` (" ++ string:join(Fields, ", ") ++ ") VALUES (" ++ string:join(Values, ", ") ++ ");".
sql(column_def, Name, Type, Len, Autoincrement, NotNull, Unique, Default) ->
  "`" ++ Name ++ "`" ++ 
  sql(type, Type, Len) ++ 
  sql(autoinc, Autoincrement) ++ 
  sql(notnull, NotNull) ++
  sql(unique, Unique) ++
  sql(default, Default).
sql(type, first) -> " LIMIT 1";
sql(type, _) -> "";
sql(where, Data) -> "WHERE " ++ Data;
sql(group, Data) -> "GROUP BY " ++ Data;
sql(order, Data) -> "ORDER BY " ++ Data;
sql(limit, Data) -> "LIMIT " ++ Data;
sql(autoinc, {ok, true}) -> " AUTO_INCREMENT PRIMARY KEY";
sql(notnull, {ok, true}) -> " NOT NULL";
sql(unique, {ok, true}) -> " UNIQUE";
sql(default, {ok, Value}) -> io_lib:format(" DEFAULT ~s", [texas_sql:to_sql_string(Value, ?STRING_SEPARATOR, ?STRING_QUOTE)]);
sql(clause, Clauses) when is_list(Clauses) ->
  lists:map(fun(Clause) -> sql(clause, Clause) end, Clauses);
sql(clause, {Type, Str, Params}) ->
  WhereClause = lists:foldl(fun({Field, Value}, Clause) ->
          estring:gsub(Clause, ":" ++ atom_to_list(Field), texas_sql:to_sql_string(Value, ?STRING_SEPARATOR, ?STRING_QUOTE))
      end, Str, Params),
  sql(Type, WhereClause);
sql(clause, {Type, Str}) ->
  sql(clause, {Type, Str, []});
sql(_, _) -> "".
