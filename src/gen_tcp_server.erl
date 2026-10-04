%%
%% Copyright (c) 2022 dushin.net
%% All rights reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(gen_tcp_server).

-export([start/4, start/3, start_link/4, start_link/3, stop/1, send/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%
%% gen_tcp_server behavior
%%

-callback init(Args :: term()) ->
    {ok, State :: term()} | {stop, Reason :: term()}.

-callback handle_receive(Socket :: term(), Packet :: binary(), State :: term()) ->
    {reply, Packet :: iolist(), NewState :: term()} | {noreply, NewState :: term()} | {close, Packet :: iolist()} | close.

-callback handle_tcp_closed(Socket :: term(), State :: term()) -> ok.

% -define(TRACE_ENABLED, true).
-include_lib("atomvm_lib/include/trace.hrl").

-record(state, {
    handler,
    handler_state,
    %% The listening socket, kept so terminate/2 can close it: the acceptor is
    %% spawned (not linked) and would otherwise keep the port bound after a
    %% restart, so the next bind fails and connections are swallowed by the
    %% orphaned acceptor.
    socket
}).

-define(DEFAULT_BIND_OPTIONS, #{
    family => inet,
    addr => any
}).
-define(DEFAULT_SOCKET_OPTIONS, #{}).
-define(SEND_RETRY_LIMIT, 20).
-define(SEND_RETRY_SLEEP_MS, 10).

%%
%% API
%%

start(BindOptions, Handler, Args) ->
    start(BindOptions, ?DEFAULT_SOCKET_OPTIONS, Handler, Args).

start(BindOptions, SocketOptions, Handler, Args) ->
    gen_server:start(?MODULE, {maps:merge(?DEFAULT_BIND_OPTIONS, BindOptions), SocketOptions, Handler, Args}, []).

start_link(BindOptions, Handler, Args) ->
    start_link(BindOptions, ?DEFAULT_SOCKET_OPTIONS, Handler, Args).

start_link(BindOptions, SocketOptions, Handler, Args) ->
    gen_server:start_link(?MODULE, {maps:merge(?DEFAULT_BIND_OPTIONS, BindOptions), SocketOptions, Handler, Args}, []).

stop(Server) ->
    gen_server:stop(Server).

-spec send(Socket :: term(), Packet :: iolist()) -> ok | {error, Reason :: term()}.
send(Socket, Packet) ->
    try_send(Socket, Packet).

%%
%% gen_server implementation
%%

%% @hidden
init({BindOptions, SocketOptions, Handler, Args}) ->
    Self = self(),
    case socket:open(inet, stream, tcp) of
        {ok, Socket} ->
            ok = set_socket_options(Socket, SocketOptions),
            case socket:bind(Socket, BindOptions) of
                ok ->
                    case socket:listen(Socket) of
                        ok ->
                            spawn(fun() -> accept(Self, Socket) end),
                            case Handler:init(Args) of
                                {ok, HandlerState} ->
                                    {ok, #state{handler = Handler, handler_state = HandlerState, socket = Socket}};
                                HandlerError ->
                                    try_close(Socket),
                                    {stop, {handler_error, HandlerError}}
                            end;
                        ListenError ->
                            try_close(Socket),
                            {stop, {listen_error, ListenError}}
                        end;
                BindError ->
                    try_close(Socket),
                    {stop, {bind_error, BindError}}
            end;
        OpenError ->
            {stop, {open_error, OpenError}}
    end;
init({Socket, Handler, Args}) ->
    Self = self(),
    case Handler:init(Args) of
        {ok, HandlerState} ->
            spawn(fun() -> loop(Self, Socket) end),
            {ok, #state{handler = Handler, handler_state = HandlerState, socket = Socket}};
        HandlerError ->
            {stop, {handler_error, HandlerError}}
    end.

%% @hidden
handle_call(_From, _Request, State) ->
    {noreply, State}.

%% @hidden
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @hidden
handle_info({tcp_closed, Socket}, State) ->
    ?TRACE("TCP Socket closed ~p", [Socket]),
    #state{handler=Handler, handler_state=HandlerState} = State,
    NewHandlerState = Handler:handle_tcp_closed(Socket, HandlerState),
    {noreply, State#state{handler_state=NewHandlerState}};
handle_info({tcp, Socket, Packet}, State) ->
    #state{handler=Handler, handler_state=HandlerState} = State,
    ?TRACE("received packet: len(~p) from ~p", [erlang:byte_size(Packet), socket:peername(Socket)]),
    case Handler:handle_receive(Socket, Packet, HandlerState) of
        {reply, ResponsePacket, ResponseState} ->
            ?TRACE("Sending reply to endpoint ~p", [socket:peername(Socket)]),
            try_send(Socket, ResponsePacket),
            {noreply, State#state{handler_state=ResponseState}};
        {noreply, ResponseState} ->
            ?TRACE("no reply", []),
            {noreply, State#state{handler_state=ResponseState}};
        {close, ResponsePacket} ->
            ?TRACE("Sending reply to endpoint ~p and closing socket: ~p", [socket:peername(Socket), Socket]),
            try_send(Socket, ResponsePacket),
            % timer:sleep(500),
            try_close(Socket),
            {noreply, State};
        close  ->
            ?TRACE("Closing socket ~p", [Socket]),
            try_close(Socket),
            {noreply, State};
        _SomethingElse ->
            ?TRACE("Unexpected response from handler ~p: ~p", [Handler, SomethingElse]),
            try_close(Socket),
            {noreply, State}
    end;
handle_info(Info, State) ->
    io:format("Received spurious info msg: ~p~n", [Info]),
    {noreply, State}.

%% @hidden
terminate(_Reason, #state{socket = Socket}) ->
    %% Closing the listening socket also stops the acceptor (its accept returns
    %% {error, closed}), which frees the port for a restart.
    try_close(Socket),
    ok.

%%
%% internal functions
%%

%% @private
try_send(Socket, Packet) when is_binary(Packet) ->
    try_send(Socket, Packet, ?SEND_RETRY_LIMIT);
try_send(Socket, Char) when is_integer(Char) ->
    %% TODO handle unicode
    ?TRACE("Sending char ~p as ~p", [Char, <<Char:8>>]),
    try_send(Socket, <<Char:8>>);
try_send(Socket, List) when is_list(List) ->
    case is_string(List) of
        true ->
            try_send(Socket, list_to_binary(List));
        _ ->
            try_send_iolist(Socket, List)
    end.

try_send(Socket, Packet, RetriesLeft) ->
    ?TRACE(
        "Trying to send binary packet data to socket ~p.  Packet (or len): ~p", [
        Socket, case byte_size(Packet) < 32 of true -> Packet; _ -> byte_size(Packet) end
    ]),
    case socket:send(Socket, Packet) of
        ok ->
            ?TRACE("sent.", []),
            ok;
        {ok, Packet} ->
            retry_send(Socket, Packet, RetriesLeft);
        {ok, Rest} ->
            ?TRACE("sent.  remaining: ~p", [Rest]),
            try_send(Socket, Rest, ?SEND_RETRY_LIMIT);
        {error, eagain} ->
            retry_send(Socket, Packet, RetriesLeft);
        Error ->
            io:format("Send failed due to error ~p~n", [Error]),
            Error
    end.

try_send_iolist(_Socket, []) ->
    ok;
try_send_iolist(Socket, [H | T]) ->
    case try_send(Socket, H) of
        ok ->
            try_send_iolist(Socket, T);
        Error ->
            Error
    end.

retry_send(_Socket, _Packet, 0) ->
    Error = {error, eagain},
    io:format("Send failed due to transient backpressure after ~p retries~n", [?SEND_RETRY_LIMIT]),
    Error;
retry_send(Socket, Packet, RetriesLeft) ->
    timer:sleep(?SEND_RETRY_SLEEP_MS),
    try_send(Socket, Packet, RetriesLeft - 1).

is_string([]) ->
    true;
is_string([H | T]) when is_integer(H) ->
    is_string(T);
is_string(_) ->
    false.

%% @private
try_close(Socket) ->
    case socket:close(Socket) of
        ok ->
            ok;
        Error ->
            io:format("Close failed due to error ~p~n", [Error])
    end.

%% @private
set_socket_options(Socket, SocketOptions) ->
    maps:fold(
        fun(Option, Value, Accum) ->
            erlang:display({setopt, Socket, Option, Value}),
            ok = socket:setopt(Socket, Option, Value),
            Accum
        end,
        ok,
        SocketOptions
    ).

%% @private
accept(ControllingProcess, ListenSocket) ->
    ?TRACE("pid ~p Waiting for connection on ~p ...", [self(), socket:sockname(ListenSocket)]),
    case socket:accept(ListenSocket) of
        {ok, Connection} ->
            ?TRACE("Accepted connection from ~p", [socket:peername(Connection)]),
            spawn(fun() -> accept(ControllingProcess, ListenSocket) end),
            loop(ControllingProcess, Connection);
        {error, closed} ->
            %% The listener is gone; there is nothing left to accept.
            ?TRACE("Listener ~p closed", [ListenSocket]);
        {error, Error} ->
            %% A Wi-Fi glitch can make accept fail (ehostunreach, enomem, ...)
            %% while the listener stays open. Returning here leaves the server
            %% bound but deaf: TCP connects are accepted by the stack and then
            %% reset, and nothing short of a restart listens again. Retry
            %% instead, with a pause so a persistent error does not spin.
            ?TRACE("Error accepting connection: ~p; retrying", [Error]),
            receive after 1000 -> ok end,
            accept(ControllingProcess, ListenSocket)
    end.


%% @private
loop(ControllingProcess, Connection) ->
    %% `socket:recv' can raise (AtomVM's select teardown does, on a socket the
    %% peer already closed) and the process would then exit without closing the
    %% connection: LWIP_MAX_SOCKETS is small, so a few leaked fds leave the
    %% listener unable to accept anything. Catch, close, and be done.
    case catch socket:recv(Connection) of
        {ok, Data} ->
            ?TRACE("Received data ~p on connection ~p", [Data, Connection]),
            ControllingProcess ! {tcp, Connection, Data},
            loop(ControllingProcess, Connection);
        {error, closed} ->
            ?TRACE("Peer closed connection ~p", [Connection]),
            try_close(Connection),
            ControllingProcess ! {tcp_closed, Connection},
            ok;
        {error, _SomethingElse} ->
            ?TRACE("Some other error occurred ~p", [Connection]),
            try_close(Connection);
        {'EXIT', _Reason} ->
            ?TRACE("recv raised on ~p; closing", [Connection]),
            try_close(Connection),
            ok
    end.
