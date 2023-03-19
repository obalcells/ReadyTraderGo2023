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
import asyncio
import itertools
import math

from typing import List, Dict, Any, Tuple, Set

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side
from ready_trader_go.order_book import OrderBook


LOT_SIZE = 10
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS

# Order
# self.client_order_id: int = client_order_id
# self.instrument: Instrument = instrument
# self.lifespan: Lifespan = lifespan
# self.side: Side = side
# self.price: int = price
# self.remaining_volume: int = volume
# self.total_fees: int = 0
# self.volume: int = volume
# self.listener: IOrderListener = listener

class CustomOrder:
    def __init__(self, order_id: int, side: Side, price: int, volume: int, sequence_number: int):
        """Initialise a new instance of the Order class."""
        self.order_id: int = order_id 
        self.side: Side = side
        self.price: int = price
        self.remaining_volume: int = volume
        self.total_fees: int = 0
        self.volume: int = volume
        self.sequence_number: int = sequence_number

    def other_str(self):
        return "[ {0} price={2} remaining={3} init_volume={4} id={5} ]".format("BUY" if self.side == Side.BUY else "SELL", "", self.price, self.remaining_volume, self.volume, self.order_id)

class AutoTrader(BaseAutoTrader):
    """Example Auto-trader.

    When it starts this auto-trader places ten-lot bid and ask orders at the
    current best-bid and best-ask prices respectively. Thereafter, if it has
    a long position (it has bought more lots than it has sold) it reduces its
    bid and ask prices. Conversely, if it has a short position (it has sold
    more lots than it has bought) then it increases its bid and ask prices.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop, config: Dict[str, Any]):
        """Initialise a new instance of the AutoTrader class."""
        super().__init__(loop, config)
        self.order_ids = itertools.count(1)

        self.bid_ids: Set[int] = set()
        self.bids: Dict[int, CustomOrder] = {}
        self.ask_ids: Set[int] = set()
        self.asks: Dict[int, CustomOrder] = {}
        self.total_bid_size = 0
        self.total_ask_size = 0

        self.hedge_bid_ids: Set[int] = set()
        self.hedge_bids: Dict[int, CustomOrder] = {}
        self.hedge_ask_ids: Set[int] = set()
        self.hedge_asks: Dict[int, CustomOrder] = dict()
        self.total_hedge_bid_size = 0
        self.total_hedge_ask_size = 0 

        self.last_hedged_sequence_number = 0
        self.first_hedged_sequence_number = -1

        self.etf_position = 0
        self.fut_position = 0
        self.profit = 0
        self.fees = 0
        self.true_price = 0

        self.etf_order_book_sequence_number = -2 
        self.fut_order_book_sequence_number = -1 

        self.etf_ask_prices = [] 
        self.etf_ask_volumes = []
        self.etf_bid_prices = [] 
        self.etf_bid_volumes = [] 
        self.fut_ask_prices = []
        self.fut_ask_volumes = []
        self.fut_bid_prices = []
        self.fut_bid_volumes = [] 

        self.last_30_etf_trades = []

        # parameters to tweak
        self.sequence_number_hedging_delay = config["Parameters"]["sequence_number_hedging_delay"] # 600 
        self.drift_delay = config["Parameters"]["drift_delay"] #3
        self.cancelling_delay = config["Parameters"]["cancelling_delay"] # 0.01
        self.gamma = config["Parameters"]["gamma"] # 0.005
        self.volume_adjustment_constant = config["Parameters"]["volume_adjustment_constant"]


    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())
        if client_order_id != 0 and (client_order_id in self.bid_ids or client_order_id in self.ask_ids):
            self.on_order_status_message(client_order_id, 0, 0, 0)
        elif client_order_id != 0:
            self.logger.info("Received an error message from an order that's no longer being tracked")

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

        # if volume = 0 the order wasn't filled
        if client_order_id in self.hedge_bid_ids:
            self.fut_position += volume
            # even if we haven't filled the order we just delete it 
            self.total_hedge_bid_size -= self.hedge_bids[client_order_id].volume
            del self.hedge_bids[client_order_id]
            self.hedge_bid_ids.remove(client_order_id)

        elif client_order_id in self.hedge_ask_ids:
            self.fut_position -= volume
            # even if we haven't filled it we just delete it
            self.total_hedge_ask_size -= self.hedge_asks[client_order_id].volume
            del self.hedge_asks[client_order_id]
            self.hedge_ask_ids.remove(client_order_id)

        self.update_hedges_if_necessary()


    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        self.logger.info("received order book for instrument %d with sequence number %d", instrument,
                         sequence_number)

        self.logger.info("Balance is {0} ETF and {1} FUT".format(self.etf_position, self.fut_position))

        if instrument == Instrument.ETF:
            self.update_etf_order_book_data(sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)
        else:
            self.update_fut_order_book_data(sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)
         
        if abs(self.etf_position - self.fut_position) <= 10:
            self.last_hedged_sequence_number = sequence_number

        self.check_conditions()

        # if we don't have order book information or it's not synchronized we don't
        # want to touch our bids/asks
        if self.market_data_ready():
            if not self.bid_ids and not self.ask_ids and self.etf_position == 0:
                self.initialize_bid_and_ask()
            else:
                self.update_bids()
                self.update_asks()

        self.check_conditions()

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("received order status change for order %d with fill_volume %d and remaining volume %d and fees %d", client_order_id,
                         fill_volume, remaining_volume, fees)

        self.log_outstanding_offers()

        if client_order_id in self.bid_ids:

            # bid was cancelled
            if remaining_volume == 0 and fill_volume != self.bids[client_order_id].volume:
                self.logger.info("Cancelled ETF | BUY | seq number {0} | order number {1} | fill_volume {2}".format(self.bids[client_order_id].sequence_number, client_order_id, self.bids[client_order_id].volume))

                self.total_bid_size -= self.bids[client_order_id].remaining_volume
                del self.bids[client_order_id]
                self.bid_ids.remove(client_order_id)

                # we will place a new bid order if the invariant isn't satisfied
                self.update_bids()

            # order was partially or fully filled
            else:
                self.logger.info("Filled ETF | BUY | seq number {0} | order number {1} | fill_volume {2} | remaining_volume {3}".format(self.bids[client_order_id].sequence_number, client_order_id, self.bids[client_order_id].volume, remaining_volume))

                amount_filled = self.bids[client_order_id].remaining_volume - remaining_volume 
                self.total_bid_size -= amount_filled
                self.etf_position += amount_filled
                self.bids[client_order_id].remaining_volume = remaining_volume

                # if order has been fully filled
                if remaining_volume == 0:
                    assert fill_volume == self.bids[client_order_id].volume 
                    del self.bids[client_order_id]
                    self.bid_ids.remove(client_order_id)

                if amount_filled > 0:
                    self.update_quotes_after_bid_filled(amount_filled)

        elif client_order_id in self.ask_ids:

            # order was cancelled
            if remaining_volume == 0 and fill_volume != self.asks[client_order_id].volume:
                self.logger.info("Cancelled ETF | SELL | seq number {0} | order number {1} | fill_volume {2} | remaining_volume {3}".format(self.asks[client_order_id].sequence_number, client_order_id, self.asks[client_order_id].volume, remaining_volume))

                self.total_ask_size -= self.asks[client_order_id].remaining_volume
                del self.asks[client_order_id]
                self.ask_ids.remove(client_order_id)

                # we will place a new ask order if the invariant isn't satisfied
                self.update_ask()

            else:
                self.logger.info("Filled ETF | SELL | seq number {0} | order number {1} | fill_volume {2} | remaining_volume {3}".format(self.asks[client_order_id].sequence_number, client_order_id, self.asks[client_order_id].volume, remaining_volume))

                amount_filled = self.asks[client_order_id].remaining_volume - remaining_volume
                self.total_ask_size -= amount_filled
                self.etf_position -= amount_filled 
                self.asks[client_order_id].remaining_volume = remaining_volume

                # if order has been fully filled
                if remaining_volume == 0:
                    assert fill_volume == self.ask_original_size 
                    del self.asks[client_order_id]
                    self.ask_ids.remove(client_order_id)

                if amount_filled > 0:
                    self.update_quotes_after_ask_filled(amount_filled)

        else:
            # we shouldn't receive order status updates of some other order
            assert False

        self.log_outstanding_offers()
        self.check_conditions()
        self.update_hedges_if_necessary()


    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.

        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.

        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)

        # TODO: Update bid/ask prices based on this information
        if instrument == Instrument.ETF:
            for i in range(len(bid_prices))[::-1]:
                if bid_volumes[i] > 0:
                    self.last_30_etf_trades.insert(0, (bid_prices[i], bid_volumes[i]))
                if ask_volumes[i] > 0:
                    self.last_30_etf_trades.insert(0, (ask_prices[i], ask_volumes[i]))
            while len(self.last_30_etf_trades) > 30:
                self.last_30_etf_trades.pop()

        self.check_conditions()

        # if we don't have order book information or it's not synchronized we don't
        # want to touch our bids/asks
        if self.market_data_ready():
            if not self.bid_ids and not self.ask_ids and self.etf_position == 0:
                self.initialize_bid_and_ask()
            else:
                self.update_bids()
                self.check_conditions()
                self.update_asks()

        self.check_conditions()

    # Functions for calculating parameters

    def initialize_bid_and_ask(self) -> None:
        new_bid_size = 50 
        new_bid_price: int = self.calculate_bid_price()
        self.place_bid(price=new_bid_price, size=new_bid_size)

        new_ask_size = 50 
        new_ask_price = self.calculate_ask_price()
        self.place_ask(price=new_ask_price, size=new_ask_size)

        self.check_conditions()

    def update_quotes_after_bid_filled(self, amount_filled) -> None:
        self.logger.info("Updating quotes after bid was filled. Quotes before updating:")
        self.log_outstanding_offers()

        self.check_conditions()

        if self.total_bid_size + self.etf_position <= POSITION_LIMIT:
            assert self.etf_position - self.total_ask_size - amount_filled >= -POSITION_LIMIT

            new_bid_price = self.calculate_bid_price()
            new_ask_price = self.calculate_ask_price()
            self.place_bid(price=new_bid_price, size=amount_filled)
            self.place_ask(price=new_ask_price, size=amount_filled)

        else:
            new_total_bid_size = 100 - self.etf_position
            new_bid_size = new_total_bid_size - self.total_bid_size 
            assert new_bid_size >= 0
            new_ask_size = 100
            new_bid_price = self.calculate_bid_price()
            new_ask_price = self.calculate_ask_price()
            self.place_bid(price=new_bid_price, size=new_bid_size)
            self.place_ask(price=new_ask_price, size=new_ask_size)

        self.check_conditions()

    def update_quotes_after_ask_filled(self, amount_filled) -> None:
        self.logger.info("Updating quotes after ask was filled. Quotes before updating:")
        self.log_outstanding_offers()

        self.check_conditions()

        if -self.total_ask_size + self.etf_position >= -POSITION_LIMIT:
            assert self.etf_position + self.total_bid_size + amount_filled <= POSITION_LIMIT

            new_bid_price = self.calculate_bid_price()
            new_ask_price = self.calculate_ask_price()
            self.place_bid(price=new_bid_price, size=amount_filled)
            self.place_ask(price=new_ask_price, size=amount_filled)

        else:
            new_total_ask_size = -100 + self.etf_position
            new_ask_size = new_total_ask_size  - self.total_ask_size
            new_bid_size = 100
            new_bid_price = self.calculate_bid_price()
            new_ask_price = self.calculate_ask_price()
            self.place_bid(price=new_bid_price, size=new_bid_size)
            self.place_ask(price=new_ask_price, size=new_ask_size)

        self.check_conditions()

    # we cancel any bids we no longer want and place a new bid (if necessary) to satisfy the invariant
    def update_bids(self) -> None:
        new_bid_price = self.calculate_bid_price()

        self.logger.info("Updating bids. Quotes before updating:")
        self.log_outstanding_offers()

        self.check_conditions()

        # first we cancel all outdated bids
        for order_id, bid in self.bids.items():
            if bid.price != new_bid_price and bid.sequence_number + self.cancelling_delay <= self.etf_order_book_sequence_number:
                self.logger.info("Cancelling BID with id {0} of remaining volume {1}".format(bid.order_id, bid.remaining_volume))
                self.send_cancel_order(bid.order_id)

        # see if we should place a new bid
        new_bid_size = self.calculate_bid_size()
        if new_bid_size > 0:
            self.place_bid(price=new_bid_price, size=new_bid_size)

        self.logger.info("Bids were updated. Quotes are now:")
        self.log_outstanding_offers()

        self.check_conditions()

    # we cancel any asks we no longer want and place a new ask (if necessary) to satisfy the invariant
    def update_asks(self) -> None:
        new_ask_price = self.calculate_ask_price()

        self.logger.info("Updating asks. Quotes before updating:")
        self.log_outstanding_offers()

        self.check_conditions()

        # first we cancel all outdated asks
        for order_id, ask in self.asks.items():
            if ask.price != new_ask_price and ask.sequence_number + self.cancelling_delay <= self.etf_order_book_sequence_number:
                self.logger.info("Cancelling ASK with id {0} of remaining volume {1}".format(ask.order_id, ask.remaining_volume))
                self.send_cancel_order(ask.order_id)

        # now we check if we should place a new ask
        new_ask_size = self.calculate_ask_size()
        if new_ask_size > 0:
            self.place_ask(price=new_ask_price, size=new_ask_size)

        self.logger.info("Asks were updated. Quotes are now:")
        self.log_outstanding_offers()

        self.check_conditions()

    def update_hedges_if_necessary(self) -> None:
        # the actual current market exposure
        current_delta = self.etf_position + self.fut_position

        # if we already have an outstanding hedge order we wait until it gets executed
        if self.hedge_bids or self.hedge_asks:
            return
            
        # if we don't have to hedge yet we don't do it
        if self.last_hedged_sequence_number + self.sequence_number_hedging_delay > self.etf_order_book_sequence_number:
            return

        if current_delta > 0:
            self.place_hedge_ask(price=MIN_BID_NEAREST_TICK, size=current_delta)

        if current_delta < 0:
            self.place_hedge_buy(price=MAX_ASK_NEAREST_TICK, size=-current_delta)

    def calculate_bid_size(self) -> int:
        bid_size = max(0, -self.etf_position + self.total_ask_size - self.total_bid_size)
        assert abs(self.etf_position + self.total_bid_size + bid_size) <= POSITION_LIMIT
        return bid_size

    def calculate_ask_size(self) -> int:
        ask_size = max(0, self.etf_position - self.total_bid_size - self.total_ask_size)
        assert abs(self.etf_position - self.total_ask_size - ask_size) <= POSITION_LIMIT
        return ask_size

    def calculate_effective_etf_midprice(self, order_size) -> float:
        if order_size == 0:
            order_size = LOT_SIZE

        volume = 0
        effective_buy_price = 0

        # the average price at which we can buy `order_size` lots
        for i in range(5):
            available_volume_here = min(order_size - volume, self.etf_ask_volumes[i])
            effective_buy_price += self.etf_ask_prices[i] * available_volume_here
            volume += available_volume_here
            if volume >= order_size:
                assert volume == order_size 
                break

        effective_buy_price /= volume

        volume = 0
        effective_sell_price = 0
        for i in range(5):
            available_volume_here = min(order_size - volume, self.etf_bid_volumes[i])
            effective_sell_price += self.etf_bid_prices[i] * available_volume_here
            volume += available_volume_here
            if volume >= order_size:
                assert volume == order_size 
                break
            
        effective_sell_price /= volume

        return (effective_buy_price + effective_sell_price) * 0.5

    # the price is given in number of tick sizes
    def calculate_bid_price(self) -> int:
        assert self.market_data_ready()

        reservation_price = self.calculate_reservation_price()

        delta = self.calculate_delta()

        reservation_price -= delta * 0.5

        rounded_price = math.floor(reservation_price / TICK_SIZE_IN_CENTS) * TICK_SIZE_IN_CENTS

        return rounded_price 

    def calculate_ask_price(self) -> int:
        assert self.market_data_ready()

        reservation_price = self.calculate_reservation_price()

        delta = self.calculate_delta()

        reservation_price += delta * 0.5

        rounded_price = math.ceil(reservation_price / TICK_SIZE_IN_CENTS) * TICK_SIZE_IN_CENTS

        return rounded_price

    # delta is the difference between our bid and ask quotes
    def calculate_delta(self) -> float:
        assert self.market_data_ready()

        sigma_squared = self.calculate_sigma_squared() 
        gamma = self.gamma
        t_diff = 1 - (self.last_hedged_sequence_number + self.sequence_number_hedging_delay - self.etf_order_book_sequence_number) / self.sequence_number_hedging_delay
        kappa = self.volume_adjustment_constant * self.calculate_volume_etf()

        delta = gamma * sigma_squared * t_diff + 2 * math.log(1 + gamma / kappa) / gamma

        return delta

    # r = s - q * gamma * sigma^2 * (T - t)
    def calculate_reservation_price(self) -> float:
        assert self.market_data_ready()

        s = self.calculate_true_price()
        q = self.etf_position
        sigma_squared = self.calculate_sigma_squared() 
        gamma = self.gamma
        t_diff = 1 - (self.last_hedged_sequence_number + self.sequence_number_hedging_delay - self.etf_order_book_sequence_number) / self.sequence_number_hedging_delay

        r = s - q * gamma * sigma_squared * t_diff

        return r

    def calculate_true_price(self) -> float:
        # true price is the midprice of the future + the difference now times some lag factor
        fut_midprice = (self.fut_ask_prices[0] + self.fut_bid_prices[0]) * 0.5

        self.true_price = fut_midprice 

        return self.true_price

    def calculate_sigma_squared(self) -> float:
        assert len(self.last_30_etf_trades) > 0

        mean_price = 0.0
        for price, volume in self.last_30_etf_trades:
            mean_price += price / TICK_SIZE_IN_CENTS
        mean_price /= len(self.last_30_etf_trades)

        sigma_squared = 0.0
        for price, volume in self.last_30_etf_trades:
            sigma_squared += ((price/TICK_SIZE_IN_CENTS - mean_price) * (price/TICK_SIZE_IN_CENTS - mean_price)) / len(self.last_30_etf_trades)
        
        self.logger.info("Calculated sigma_squared is {0}".format(sigma_squared))
        output_str = ""
        for price, volume in self.last_30_etf_trades:
            output_str += str(price/TICK_SIZE_IN_CENTS) + "  "
        self.logger.info("Last ETF traded prices {0}".format(output_str))

        sigma_squared *= TICK_SIZE_IN_CENTS

        return sigma_squared 

    # computes the total volume of the last 30 etf trades
    def calculate_volume_etf(self) -> int:
        total_volume = 0

        for price, volume in self.last_30_etf_trades:
            total_volume += volume

        return total_volume





    # Helper functions
        
    def place_bid(self, price, size) -> None:
        bid = CustomOrder(order_id=next(self.order_ids), side=Side.BUY, price=price, volume=size, sequence_number=self.etf_order_book_sequence_number)
        self.bid_ids.add(bid.order_id)
        self.bids[bid.order_id] = bid
        self.total_bid_size += size
        self.logger.info("Inserting ETF | BUY | seq number {0} | order number {1} | size {2} | price {3}".format(bid.sequence_number, bid.order_id, bid.volume, bid.price))
        self.send_insert_order(client_order_id=bid.order_id, side=bid.side, price=bid.price, volume=bid.volume, lifespan=Lifespan.GOOD_FOR_DAY)

    def place_ask(self, price, size) -> None:
        ask = CustomOrder(order_id=next(self.order_ids), side=Side.SELL, price=price, volume=size, sequence_number=self.etf_order_book_sequence_number)
        self.ask_ids.add(ask.order_id)
        self.asks[ask.order_id] = ask
        self.total_ask_size += size
        self.logger.info("Inserting ETF | SELL | seq number {0} | order number {1} | size {2} | price {3}".format(ask.sequence_number, ask.order_id, ask.volume, ask.price))
        self.send_insert_order(client_order_id=ask.order_id, side=ask.side, price=ask.price, volume=ask.volume, lifespan=Lifespan.GOOD_FOR_DAY)

    def place_hedge_bid(self, price, size):
        hedge_bid = CustomOrder(order_id=next(self.order_ids), side=Side.BUY, price=price, volume=size, sequence_number=self.etf_order_book_sequence_number)
        self.hedge_bid_ids.add(hedge_bid.order_id)
        self.hedge_asks[hedge_bid.order_id] = hedge_bid
        self.total_hedge_bid_size += size
        self.logger.info("Inserting FUT | BUY | seq number {0} | order number {1} | size {2} | price {3}".format(hedge_bid.sequence_number, hedge_bid.order_id, hedge_bid.volume, hedge_bid.price))
        self.send_hedge_order(client_order_id=hedge_bid.order_id, side=hedge_bid.side, price=hedge_bid.price, volume=hedge_bid.volume)

    def place_hedge_ask(self, price, size):
        hedge_ask = CustomOrder(order_id=next(self.order_ids), side=Side.SELL, price=price, volume=size, sequence_number=self.etf_order_book_sequence_number)
        self.hedge_ask_ids.add(hedge_ask.order_id)
        self.hedge_asks[hedge_ask.order_id] = hedge_ask
        self.total_hedge_ask_size += size
        self.logger.info("Inserting FUT | SELL | seq number {0} | order number {1} | size {2} | price {3}".format(hedge_ask.sequence_number, hedge_ask.order_id, hedge_ask.volume, hedge_ask.price))
        self.send_hedge_order(client_order_id=hedge_ask.order_id, side=hedge_ask.side, price=hedge_ask.price, volume=hedge_ask.volume)

    # these conditions have to be fulfilled at all times
    def check_conditions(self) -> None:
        assert (abs(self.etf_position) <= POSITION_LIMIT and abs(self.fut_position) <= POSITION_LIMIT)
        assert (self.etf_position + self.total_bid_size <= POSITION_LIMIT and self.etf_position - self.total_ask_size >= -POSITION_LIMIT)
        assert (self.etf_position + self.total_bid_size <= POSITION_LIMIT and self.etf_position - self.total_ask_size >= -POSITION_LIMIT)
        assert (self.total_bid_size - self.total_ask_size + self.etf_position == 0)
        for order_id, bid in self.bids.items():
            assert bid.remaining_volume > 0 and bid.volume > 0
        for order_id, ask in self.asks.items():
            assert ask.remaining_volume > 0 and ask.volume > 0

    # unless this function returns true we won't place any bid/asks 
    def market_data_ready(self) -> bool:
        if self.etf_order_book_sequence_number != self.fut_order_book_sequence_number:
            return False
        if self.etf_bid_volumes[0] == 0 or self.etf_bid_prices[0] == 0:
            return False
        if self.etf_ask_volumes[0] == 0 or self.etf_ask_prices[0] == 0:
            return False
        if self.fut_bid_volumes[0] == 0 or self.fut_bid_prices[0] == 0:
            return False
        if self.fut_ask_volumes[0] == 0 or self.fut_ask_prices[0] == 0:
            return False
        if len(self.last_30_etf_trades) == 0:
            return False
        return True

    def update_etf_order_book_data(self, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        self.etf_order_book_sequence_number = sequence_number 
        self.etf_ask_prices = ask_prices
        self.etf_ask_volumes = ask_volumes
        self.etf_bid_prices = bid_prices
        self.etf_bid_volumes = bid_volumes
        
    def update_fut_order_book_data(self, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        self.fut_order_book_sequence_number = sequence_number 
        self.fut_ask_prices = ask_prices
        self.fut_ask_volumes = ask_volumes
        self.fut_bid_prices = bid_prices
        self.fut_bid_volumes = bid_volumes

    def log_outstanding_offers(self):
        active_bids = "ETF active bids " 
        active_asks = "ETF active asks "

        for order_id, bid in self.bids.items():
            active_bids += bid.other_str() + " "
        for order_id, ask in self.asks.items():
            active_asks += ask.other_str() + " "

        self.logger.info(active_bids)
        self.logger.info(active_asks)
