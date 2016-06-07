.PHONY: doc
REBAR = ./rebar3

compile:
	@$(REBAR) compile

tests:
	@$(REBAR) eunit

elixir:
	@$(REBAR) elixir generate_mix
	@$(REBAR) elixir generate_lib

dist: compile tests elixir

distclean:
	@rm -rf _build rebar.lock mix.lock test/eunit

