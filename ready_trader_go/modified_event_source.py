# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader Go.
#
#     Ready Trader Go is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader Go is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader Go.  If not, see
#     <https://www.gnu.org/licenses/>.
import collections
import csv
import itertools

from typing import Callable, Dict, Iterator, List, NamedTuple, Optional, Set, TextIO, Tuple

from ready_trader_go.account import AccountFactory, CompetitorAccount
from ready_trader_go.messages import (AMEND_EVENT_MESSAGE, AMEND_EVENT_MESSAGE_SIZE, CANCEL_EVENT_MESSAGE,
                                      CANCEL_EVENT_MESSAGE_SIZE, ERROR_MESSAGE, ERROR_MESSAGE_SIZE, HEADER_SIZE,
                                      HEDGE_EVENT_MESSAGE, HEDGE_EVENT_MESSAGE_SIZE, INSERT_EVENT_MESSAGE,
                                      INSERT_EVENT_MESSAGE_SIZE, LOGIN_EVENT_MESSAGE, LOGIN_EVENT_MESSAGE_SIZE,
                                      TRADE_EVENT_MESSAGE, TRADE_EVENT_MESSAGE_SIZE, MessageType)
from ready_trader_go.order_book import TOP_LEVEL_COUNT, Order, OrderBook
from ready_trader_go.types import Instrument, Lifespan, Side


__all__ = ("ModifiedRecordedEventSource")


TICK_INTERVAL_MILLISECONDS = 500
TICK_INTERVAL_SECONDS = TICK_INTERVAL_MILLISECONDS / 1000.0

class Event(NamedTuple):
    """A recorded event."""
    when: float
    emitter: Callable
    args: Tuple


class ModifiedRecordedEventSource():
    """A source of events taken from a recording of a match."""

    def __init__(self, etf_clamp: float, tick_size: float):
        """Initialise a new instance of the class."""

        self._account_factory: AccountFactory = AccountFactory(etf_clamp, tick_size)
        self.__teams: Set[str] = set()
        self.__end_time: float = 0.0
        self.__events: List[Event] = list()
        self.__event_iter: Optional[Iterator] = None
        self.__next_event: Optional[Event] = None
        self.__now: float = 0.0
        self.__order_books: Tuple[List[int], ...] = tuple(list() for _ in Instrument)

    def _on_timer_tick(self):
        """Callback when the timer ticks."""
        now = self.__now = self.__now + TICK_INTERVAL_SECONDS

        if self.__next_event.when <= now:
            self.__next_event.emitter(*self.__next_event.args)
            event: Optional[Event] = None
            for event in self.__event_iter:
                if event.when > now:
                    break
                event.emitter(*event.args)
            self.__next_event = event

        tick = int(now // TICK_INTERVAL_SECONDS)
        for i in Instrument:
            if len(self.__order_books[i]) >= (tick + 1) * 4 * TOP_LEVEL_COUNT:
                data = (self.__order_books[i][j * TOP_LEVEL_COUNT:(j + 1) * TOP_LEVEL_COUNT]
                        for j in range(tick * 4, (tick + 1) * 4))
                # self.order_book_changed.emit(i, now, *data)

        # if self.__now >= self.__end_time:
        #     self._timer.stop()
        #     self.match_over.emit()

    @staticmethod
    def from_csv(file_object: TextIO, etf_clamp: float, tick_size: float):
        """Create a new RecordedEventSource instance from a CSV file."""
        source = ModifiedRecordedEventSource(etf_clamp, tick_size)
        events = source.__events

        reader = csv.reader(file_object)
        next(reader)  # Skip header

        accounts: Dict[str, CompetitorAccount] = collections.defaultdict(source._account_factory.create)
        books: Tuple[OrderBook, ...] = tuple(OrderBook(i, 0.0, 0.0) for i in Instrument)
        orders: Dict[str, Dict[int, Order]] = collections.defaultdict(dict)

        ask_prices = [0] * TOP_LEVEL_COUNT
        ask_volumes = [0] * TOP_LEVEL_COUNT
        bid_prices = [0] * TOP_LEVEL_COUNT
        bid_volumes = [0] * TOP_LEVEL_COUNT

        def take_snapshot(when: float):
            for i in Instrument:
                # events.append(Event(when, source.midpoint_price_changed.emit, (i, when, books[i].midpoint_price())))
                books[i].top_levels(ask_prices, ask_volumes, bid_prices, bid_volumes)
                source.__order_books[i].extend(itertools.chain(ask_prices, ask_volumes, bid_prices, bid_volumes))

            future_price: int = books[Instrument.FUTURE].last_traded_price()
            etf_price: int = books[Instrument.ETF].last_traded_price()
            if future_price is not None and etf_price is not None:
                for team, account in accounts.items():
                    account.update(future_price, etf_price)
                    # events.append(Event(when, source.profit_loss_changed.emit,
                    #                     (team, when, account.profit_or_loss / 100.0, account.etf_position,
                    #                      account.future_position, account.account_balance / 100.0,
                    #                      account.total_fees / 100.0)))

        target_team = "Shedneryan_223251"

        now: float = TICK_INTERVAL_SECONDS
        for row in reader:
            tm = float(row[0])

            team: str = row[1]

            if tm > now:
                take_snapshot(now)
                now += TICK_INTERVAL_SECONDS
            elif team == target_team:
                take_snapshot(now)

            if team == target_team:
                print("*****************")
                print("")
                print("")
                print("His account balance is | ETF position {0} | FUT position {1} | PnL {2} |".format(accounts[team].etf_position, accounts[team].future_position, accounts[team].profit_or_loss))
                print("                       | ETF BuyVol   {0} | ETF SellVol  {1} |".format(accounts[team].buy_volume, accounts[team].sell_volume))
                active_bids = "ETF active bids " 
                active_asks = "ETF active asks "
                for order_id in orders[team].keys():
                    order = orders[team][order_id]
                    if order.side == Side.BUY:
                        active_bids += order.other_str()
                    else:
                        active_asks += order.other_str()
                print(active_bids)
                print(active_asks)
                print("ETF best bids   " + "[ {0} {1} {2} {3} {4} ]".format(*bid_prices))
                print("                [ {0} {1} {2} {3} {4} ]".format(*bid_volumes))
                print("ETF best asks   " + "[ {0} {1} {2} {3} {4} ]".format(*ask_prices))
                print("                [ {0} {1} {2} {3} {4} ]".format(*ask_volumes))
                print("ETF Midprice is {0}  FUT Midprice is {1}".format(books[Instrument.ETF].midpoint_price(), books[Instrument.FUTURE].midpoint_price()))


            order_id: int = int(row[3])
            operation: str = row[2]

            if team and team not in source.__teams:
                source.__teams.add(team)

            if operation == "Insert":
                order = Order(order_id, Instrument(int(row[4])), Lifespan[row[8]], Side[row[5]],
                              int(row[7]), int(row[6]))
                books[order.instrument].insert(tm, order)
                orders[team][order_id] = order

                if team == target_team:
                    print("{0} is INSERTING the order {1}".format(team, order.other_str()))

                # events.append(Event(tm, source.order_inserted.emit, (team, tm, order_id, order.instrument,
                #                                                      order.side, order.volume, order.price,
                #                                                      order.lifespan)))
            elif operation == "Amend":
                order = orders[team][order_id]
                volume_delta = int(row[6])
                books[order.instrument].amend(tm, order, order.volume + volume_delta)

                if team == target_team:
                    print("{0} is AMENDING by volume {1} the order {2}".format(team, volume_delta, str(order)))

                if order.remaining_volume == 0:
                    del orders[team][order_id]
                # events.append(Event(tm, source.order_amended.emit, (team, tm, order_id, volume_delta)))
            elif operation == "Cancel":
                order = orders[team].pop(order_id, None)
                if order:
                    books[order.instrument].cancel(tm, order)

                if team == target_team:
                    print("{0} is CANCELING the order {1}".format(team, order.other_str()))

                # events.append(Event(tm, source.order_cancelled.emit, (team, tm, order_id)))
            else:  # operation is "Hedge" or "Trade"
                instrument = Instrument(int(row[4]))
                side = Side[row[5]]
                volume = int(row[6])
                price = float(row[7]) if operation == "Hedge" else int(row[7])
                fee = int(row[9]) if row[9] else 0
                if team == target_team:
                    if operation == "Hedge":
                        print("{0} is HEDGING the volume {1} at price {2} and side {3}".format(team, volume, price, side))
                    else:   
                        if side == Side.BUY:
                            print("{0} executes BUY at the volume {1} at price {2} with his order {3}".format(team, volume, price, order.other_str()))
                        else:
                            print("{0} executes SELL at the volume {2} at price {3} with his order {3}".format(team, volume, price, order.other_str()))
                accounts[team].transact(instrument, side, price, volume, fee)
                if operation == "Trade":
                    if order_id in orders[team] and orders[team][order_id].remaining_volume == 0:
                        del orders[team][order_id]
                    # events.append(Event(tm, source.trade_occurred.emit, (team, tm, order_id, side, volume, price,
                    #                                                      fee)))



        take_snapshot(now)
        source.__end_time = now

        return source

    def start(self) -> None:
        """Start this recorded event source."""
        self.__now = 0.0
        # self._timer.start(TICK_INTERVAL_MILLISECONDS)
        self.__event_iter = iter(self.__events)
        self.__next_event = next(self.__event_iter, None)
        # for competitor in sorted(self.__teams):
        #     self.login_occurred.emit(competitor)
