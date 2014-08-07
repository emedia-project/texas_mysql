-module(texas_mysql).

-export([start/0]).
-export([connect/6, exec/2, close/1]).
-export([create_table/2, create_table/3, drop_table/2]).
-export([insert/3, select/4, update/4, delete/3]).
-export([string_separator/0, string_quote/0, field_separator/0]).
-export([where_null/2, set_null/1]).

-define(STRING_SEPARATOR, $").
-define(STRING_QUOTE, $\\).
-define(FIELD_SEPARATOR, $`).

-type connection() :: any().
-type err() :: any().
-type tablename() :: atom().
-type data() :: any().
-type clause_type() :: where | group | order | limit.
-type clause() :: {clause_type(), string(), [tuple()]} |
                  {clause_type(), string(), []}.
-type clauses() :: [clause()] | [].

string_separator() -> ?STRING_SEPARATOR.
string_quote()     -> ?STRING_QUOTE.
field_separator()  -> ?FIELD_SEPARATOR.
where_null("=", Key)  -> io_lib:format("~s IS NULL", [texas_sql:sql_field(Key, ?MODULE)]);
where_null(_, Key)  -> io_lib:format("~s IS NOT NULL", [texas_sql:sql_field(Key, ?MODULE)]).
set_null(Key)  -> io_lib:format("~s = NULL", [texas_sql:sql_field(Key, ?MODULE)]).

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
    emysql:remove_pool(texas:connection(Conn))
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
              Table:'-type'(Field),
              Table:'-len'(Field),
              Table:'-autoincrement'(Field),
              Table:'-not_null'(Field),
              Table:'-unique'(Field),
              Table:'-default'(Field))
        end, Table:'-fields'())),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of 
    {ok_packet, _, _, _, _, _, _} -> ok;
    _ -> error
  end.

-spec create_table(connection(), tablename(), list()) -> ok | error.
create_table(Conn, Table, Fields) ->
  SQLCmd = sql(
    create_table, 
    atom_to_list(Table), 
    lists:map(fun({Field, Options}) ->
            sql(
              column_def, 
              atom_to_list(Field),
              texas_sql:get_option(type, Options),
              texas_sql:get_option(len, Options),
              texas_sql:get_option(autoincrement, Options),
              texas_sql:get_option(not_null, Options),
              texas_sql:get_option(unique, Options),
              texas_sql:get_option(default, Options))
        end, Fields)),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of 
    {ok_packet, _, _, _, _, _, _} -> ok;
    _ -> error
  end.

-spec drop_table(connection(), tablename()) -> ok | error.
drop_table(Conn, Table) ->
  SQLCmd = "DROP TABLE IF EXISTS " ++ atom_to_list(Table),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of 
    {ok_packet, _, _, _, _, _, _} -> ok;
    _ -> error
  end.

-spec insert(connection(), tablename(), data() | list()) -> data() | ok | {error, err()}.
insert(Conn, Table, Record) ->
  SQLCmd = "INSERT INTO " ++ 
           texas_sql:sql_field(Table, ?MODULE) ++
           texas_sql:insert_clause(Record, ?MODULE),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok_packet, _, _, ID, _, _, _} ->
      case texas_sql:defined_table(Table) of
        true ->
          case Table:'-table_pk_id'() of
            {none, null} -> Record;
            {ok, Col} -> select(Conn, Table, first, [{where, [{Col, ID}]}])
          end;
        false -> ok
      end;
    {error_packet, _, _, _, E} -> {error, E}
  end.

-spec select(connection(), tablename(), first | all, clauses()) -> 
  data() | [data()] | [] | {error, err()}.
select(Conn, Table, Type, Clauses) -> 
  SQLCmd = "SELECT * FROM " ++
           texas_sql:sql_field(Table, ?MODULE) ++
           texas_sql:where_clause(texas_sql:clause(where, Clauses), ?MODULE) ++
           case Type of
             first -> " LIMIT 1";
             _ -> ""
           end,
           % TODO : add GROUP BY, ORDER BY, LIMIT
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {result_packet, _, _, [], _} -> [];
    {result_packet, _, _, _, _} = Result -> 
      case Type of
        first -> 
          [Data|_] = emysql:as_json(Result),
          case texas_sql:defined_table(Table) of
            true -> Table:new(Conn, assoc(Table, Data));
            _ -> assoc(Data)
          end;
        _ ->
          lists:map(fun(Data) ->
                case texas_sql:defined_table(Table) of
                  true -> Table:new(Conn, assoc(Table, Data));
                  _ -> assoc(Data)
                end
            end, emysql:as_json(Result))
      end;
    {error_packet, _, _, _, E} -> {error, E}
  end.

-spec update(connection(), tablename(), data(), [tuple()]) -> [data()] | {error, err()}.
update(Conn, Table, Record, UpdateData) ->
  SQLCmd = "UPDATE " ++
           texas_sql:sql_field(Table, ?MODULE) ++
           texas_sql:set_clause(UpdateData, ?MODULE) ++
           texas_sql:where_clause(Record, ?MODULE),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok_packet, _, _, _, _, _, _} ->
      UpdateRecord = lists:foldl(fun({Field, Value}, Rec) ->
              Rec:Field(Value)
          end, Record, UpdateData),
      select(Conn, Table, all, [{where, UpdateRecord}]);
    {error_packet, _, _, _, E} -> {error, E}
  end.

-spec delete(connection(), tablename(), data()) -> ok | {error, err()}.
delete(Conn, Table, Record) ->
  SQLCmd = "DELETE FROM " ++
           texas_sql:sql_field(Table, ?MODULE) ++
           texas_sql:where_clause(Record, ?MODULE),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    {ok_packet, _, _, _, _, _, _} -> ok;
    {error_packet, _, _, _, E} -> {error, E}
  end.

% Private --

exec(SQL, Conn) ->
  emysql:execute(texas:connection(Conn), list_to_binary(SQL)).

assoc(Table, Datas) ->
  lists:map(fun({Col, Value}) ->
        ACol = binary_to_atom(Col, utf8),
        case Value of
          null -> {ACol, undefined};
          _ -> {ACol, texas_type:to(Table:'-type'(ACol), Value)}
        end
    end, Datas).
assoc(Datas) ->
  lists:map(fun({Col, Value}) ->
        ACol = binary_to_atom(Col, utf8),
        case Value of
          null -> {ACol, undefined};
          _ -> {ACol, Value}
        end
    end, Datas).

sql(create_table, Name, ColDefs) -> 
  "CREATE TABLE IF NOT EXISTS `" ++ Name ++ "` (" ++ string:join(ColDefs, ", ") ++ ");";
sql(type, id, {ok, Len}) -> " INTEGER(" ++ integer_to_list(Len) ++ ")";
sql(type, id, _) -> " INTEGER";
sql(type, integer, {ok, Len}) -> " INTEGER(" ++ integer_to_list(Len) ++ ")";
sql(type, integer, _) -> " INTEGER";
sql(type, string, {ok, Len}) -> " VARCHAR(" ++ integer_to_list(Len) ++ ")";
sql(type, string, _) -> " TEXT";
sql(type, float, _) -> " REAL";
sql(type, date, _) -> " DATE";
sql(type, time, _) -> " TIME";
sql(type, datetime, _) -> " DATETIME";
sql(type, _, _) -> " TEXT";
sql(default, date, {ok, now}) -> " DEFAULT CURRENT_DATE";
sql(default, time, {ok, now}) -> " DEFAULT CURRENT_TIME";
sql(default, datetime, {ok, now}) -> " DEFAULT CURRENT_TIMESTAMP";
sql(default, _, {ok, Value}) -> io_lib:format(" DEFAULT ~s", [texas_sql:sql_string(Value, ?MODULE)]);
sql(default, _, _) -> "".
sql(column_def, Name, Type, Len, Autoincrement, NotNull, Unique, Default) ->
  "`" ++ Name ++ "`" ++ 
  sql(type, Type, Len) ++ 
  sql(autoinc, Autoincrement) ++ 
  sql(notnull, NotNull) ++
  sql(unique, Unique) ++
  sql(default, Type, Default).
sql(autoinc, {ok, true}) -> " AUTO_INCREMENT PRIMARY KEY";
sql(notnull, {ok, true}) -> " NOT NULL";
sql(unique, {ok, true}) -> " UNIQUE";
sql(_, _) -> "".
