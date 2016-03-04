PROJECT = texas_mysql

DEPS = emysql lager
dep_emysql = git https://github.com/processone/Emysql.git master
dep_lager = git https://github.com/basho/lager.git master

include erlang.mk

