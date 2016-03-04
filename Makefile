PROJECT = texas_mysql

DEPS = emysql lager texas_adapter
dep_emysql = git https://github.com/processone/Emysql.git master
dep_lager = git https://github.com/basho/lager.git master
dep_texas_adapter = git https://github.com/emedia-project/texas_adapter.git master

include erlang.mk

