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

from typing import List, Dict, Any, Tuple

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side


LOT_SIZE = 10
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS


class AutoTrader(BaseAutoTrader):
    """Example Auto-trader.

    When it starts this auto-trader places ten-lot bid and ask orders at the
    current best-bid and best-ask prices respectively. Thereafter, if it has
    a long position (it has bought more lots than it has sold) it reduces its
    bid and ask prices. Conversely, if it has a short position (it has sold
    more lots than it has bought) then it increases its bid and ask prices.
    """

    # TODO: Add this initialization function before sending code
    # def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
    #     """Initialise a new instance of the AutoTrader class."""
    #     super().__init__(loop, team_name, secret)
    #     self.order_ids = itertools.count(1)
    #     self.bids = set()
    #     self.asks = set()
    #     self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0

    def __init__(self, loop: asyncio.AbstractEventLoop, config: Dict[str, Any]):
        """Initialise a new instance of the AutoTrader class."""
        super().__init__(loop, config)
        self.order_ids = itertools.count(1)

        # We assume we have at most one outstanding order
        self.bid_price: int = 0
        self.bid_size: int = 0
        self.bid_original_size: int = 0
        self.bid_id = self.bid_sequence_number = 0

        self.ask_price: int = 0
        self.ask_size: int = 0
        self.ask_original_size: int = 0
        self.ask_id = self.ask_sequence_number = 0

        self.hedge_bid_price = self.hedge_bid_size = self.hedge_bid_id = 0
        self.hedge_ask_price = self.hedge_ask_size = self.hedge_ask_id = 0
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

        self.last_etf_traded_prices = []

        # parameters to tweak
        self.adjust_order_enabled = True
        self.drift_delay = 3 
        self.true_price_calculation = "MID_PRICE" # other option is "RUNNING_AVERAGE_TRADE_PRICE"
        self.sequence_number_heding_delay = 600 
        self.sequence_numbers_to_be_sure_we_are_hedged = 2
        self.depth_long = 30 
        self.depth_short = 5 
        self.cancelling_delay = 0 

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())
        if client_order_id != 0 and (client_order_id == self.bid_id or client_order_id == self.ask_id):
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
        if client_order_id == self.hedge_bid_id:
            self.fut_position += volume

            # even if we haven't filled the order we just delete it 
            self.hedge_bid_id = 0
            self.hedge_bid_size = 0
            self.hedge_bid_price = 0

        elif client_order_id == self.hedge_ask_id:
            self.fut_position -= volume

            self.hedge_ask_id = 0
            self.hedge_ask_size = 0
            self.hedge_ask_price = 0


    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        # self.logger.info("received order book for instrument %d with sequence number %d", instrument,
        #                  sequence_number)

        self.logger.info("Balance is {0} ETF and {1} FUT".format(self.etf_position, self.fut_position))

        if instrument == Instrument.ETF:
            self.update_etf_order_book_data(sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)
        else:
            self.update_fut_order_book_data(sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)
         
        if abs(self.etf_position - self.fut_position) <= 10:
            self.last_hedged_sequence_number = sequence_number

        # we might need to add this again if we get kicked out for not hedging
        # if abs(self.etf_position - self.fut_position) <= 10 and self.first_hedged_sequence_number == -1:
        #     self.first_hedged_sequence_number = sequence_number
        # elif abs(self.etf_position - self.fut_position) <= 10 and self.first_hedged_sequence_number + self.sequence_numbers_to_be_sure_we_are_hedged <= sequence_number:
        #     self.last_hedged_sequence_number = sequence_number
        #     self.first_hedged_sequence_number = -1

        self.check_conditions()

        if self.bid_size == 0 and self.ask_size == 0 and self.etf_position == 0:
            self.initialize_bid_and_ask()
        else:
            self.update_bid()
            self.update_ask()

        self.check_conditions()

    # refresh our bid/ask prices if we don't have any outstanding one 
    def refresh_bid_and_ask_quotes(self) -> None:
        # if we don't have order book information or it's not synchronized we don't
        # want to touch our bid/ask prices
        if self.etf_order_book_sequence_number != self.fut_order_book_sequence_number:
            return 

        self.check_conditions()

        # we keep the sizes constant
        new_bid_size = self.bid_size 
        new_bid_price = self.calculate_bid_price()

        # we keep the sizes constant
        new_ask_size = self.ask_size
        new_ask_price = self.calculate_ask_price()

        # see if we should cancel the outstanding bid
        if self.bid_size > 0:
            # self.drift_delay is set to 0 such that we will cancel very quickly
            if self.etf_order_book_sequence_number >= self.bid_sequence_number + self.drift_delay and self.bid_price != new_bid_price:
                self.send_cancel_order(self.bid_id)
        # see if we should place a new bid given that we don't have any outstanding one
        elif new_bid_size > 0 and new_bid_price > 0:
            self.bid_id = next(self.order_ids)
            self.bid_price = new_bid_price
            self.bid_size = new_bid_size
            self.bid_original_size = self.bid_size
            self.bid_sequence_number = self.etf_order_book_sequence_number
            self.logger.info("Inserting ETF | BUY | seq number {0} | order number {1} | size {2} | price {3}".format(self.bid_sequence_number, self.bid_id, self.bid_size, self.bid_price))
            assert self.etf_position + self.bid_size <= POSITION_LIMIT
            self.send_insert_order(self.bid_id, Side.BUY, self.bid_price, self.bid_size, Lifespan.GOOD_FOR_DAY)


        # see if we should cancel the ask order
        if self.ask_size > 0:
            if self.etf_order_book_sequence_number >= self.ask_sequence_number + self.drift_delay and self.ask_price != new_ask_price:
                self.send_cancel_order(self.ask_id)
        # see if we should place a new ask given that we don't have any outstanding one
        elif new_ask_size > 0 and new_ask_price > 0:
            self.ask_id = next(self.order_ids)
            self.ask_price = new_ask_price
            self.ask_size = new_ask_size
            self.ask_original_size = self.ask_size
            self.ask_sequence_number = self.etf_order_book_sequence_number
            self.logger.info("Inserting ETF | SELL | seq number {0} | order number {1} | size {2} | price {3}".format(self.ask_sequence_number, self.ask_id, self.ask_size, self.ask_price))
            assert self.etf_position - self.ask_size >= -POSITION_LIMIT
            self.send_insert_order(self.ask_id, Side.SELL, self.ask_price, self.ask_size, Lifespan.GOOD_FOR_DAY)

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        # self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
        #                  price, volume)

    def log_outstanding_offers(self):
        self.logger.info("ETF outstanding bid size {0} and ask size {1}".format(self.bid_size, self.ask_size))

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("ETF outstanding bid size {0} and ask size {1}".format(self.bid_size, self.ask_size))

        if client_order_id == self.bid_id:
            self.logger.info("Filled ETF | BUY | seq number {0} | order number {1} | fill_volume {2} | remaining_volume {3}".format(self.bid_sequence_number, self.bid_id, fill_volume, remaining_volume))

            # bid was cancelled
            if remaining_volume == 0 and fill_volume != self.bid_original_size:
                self.bid_id = 0
                self.bid_size = 0
                self.bid_price = 0
                self.bid_original_size = 0
            # order was partially or fully filled
            else:
                amount_filled = self.bid_size - remaining_volume
                self.etf_position += amount_filled 
                self.bid_size = remaining_volume

                # if order has been fully filled
                if self.bid_size == 0:
                    assert fill_volume == self.bid_original_size 
                    self.bid_id = 0
                    self.bid_price = 0
                    self.bid_sequence_number = 0
                    self.bid_original_size = 0

            self.logger.info("Updating BID after a status update from our current bid happened")
            self.update_bid()

        elif client_order_id == self.ask_id:
            self.logger.info("Filled ETF | SELL | seq number {0} | order number {1} | fill_volume {2} | remaining_volume {3}".format(self.ask_sequence_number, self.ask_id, fill_volume, remaining_volume))

            # order was cancelled
            if remaining_volume == 0 and fill_volume != self.ask_original_size:
                self.ask_id = 0
                self.ask_size = 0
                self.ask_price = 0
                self.ask_original_size = 0
            else:
                amount_filled = self.ask_size - remaining_volume
                self.etf_position -= amount_filled 
                self.ask_size = remaining_volume

                if self.ask_size == 0:
                    assert fill_volume == self.ask_original_size 
                    self.ask_id = 0
                    self.ask_price = 0
                    self.ask_sequence_number = 0
                    self.ask_original_size = 0

            self.update_ask()

        else:
            # we shouldn't receive order status updates of some other order
            assert False

        self.check_conditions()
        self.logger.info("ETF outstanding bid size {0} and ask size {1}".format(self.bid_size, self.ask_size))
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
        # self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
        #                  sequence_number)

        # TODO: Update bid/ask prices based on this information
        if instrument == Instrument.ETF:
            for i in range(len(bid_prices))[::-1]:
                if bid_volumes[i] > 0:
                    self.last_etf_traded_prices.insert(0, (bid_prices[i], bid_volumes[i]))
                if ask_volumes[i] > 0:
                    self.last_etf_traded_prices.insert(0, (ask_prices[i], ask_volumes[i]))
            while len(self.last_etf_traded_prices) > self.depth_long:
                self.last_etf_traded_prices.pop()

        self.check_conditions()

        if self.bid_size == 0 and self.ask_size == 0 and self.etf_position == 0:
            self.initialize_bid_and_ask()
        else:
            if self.bid_size == 0:
                # it will create a new appropiate bid
                self.update_bid()
            if self.ask_size == 0:
                self.update_ask()

        self.check_conditions()

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

    def initialize_bid_and_ask(self) -> None:
        assert self.bid_size == 0 and self.ask_size == 0

        if self.etf_order_book_sequence_number != self.fut_order_book_sequence_number:
            return

        new_bid_size = 50 
        new_bid_price: int = self.calculate_bid_price()

        new_ask_size = 50 
        new_ask_price = self.calculate_ask_price()

        self.bid_id = next(self.order_ids)
        self.bid_price = new_bid_price
        self.bid_size = new_bid_size
        self.bid_original_size = self.bid_size
        self.bid_sequence_number = self.etf_order_book_sequence_number
        self.logger.info("Inserting ETF | BUY | seq number {0} | order number {1} | size {2} | price {3}".format(self.bid_sequence_number, self.bid_id, self.bid_size, self.bid_price))
        assert self.etf_position + self.bid_size <= POSITION_LIMIT
        self.send_insert_order(self.bid_id, Side.BUY, self.bid_price, self.bid_size, Lifespan.GOOD_FOR_DAY)

        self.ask_id = next(self.order_ids)
        self.ask_price = new_ask_price
        self.ask_size = new_ask_size
        self.ask_original_size = self.ask_size
        self.ask_sequence_number = self.etf_order_book_sequence_number
        self.logger.info("Inserting ETF | SELL | seq number {0} | order number {1} | size {2} | price {3}".format(self.ask_sequence_number, self.ask_id, self.ask_size, self.ask_price))
        assert self.etf_position - self.ask_size >= -POSITION_LIMIT
        self.send_insert_order(self.ask_id, Side.SELL, self.ask_price, self.ask_size, Lifespan.GOOD_FOR_DAY)
        
    # def update_bid_and_ask(self, bid_volume_filled, ask_volume_filled) -> None:
    #     # if we don't have order book information or it's not synchronized we don't
    #     # want to touch our bid/ask prices
    #     if self.etf_order_book_sequence_number != self.fut_order_book_sequence_number:
    #         return 

    #     self.check_conditions()

    #     new_bid_size = self.calculate_bid_size()
    #     new_bid_price = self.calculate_bid_price(new_bid_size)

    #     new_ask_size = self.calculate_ask_size()
    #     new_ask_price = self.calculate_ask_price(new_ask_size)

    #     # see if we should amend the bid
    #     if self.bid_size > 0 and self.bid_price == new_bid_price and self.bid_size > new_bid_size: 
    #         # TODO: If self.bid_size < new_bid_size then create a new bid order instead of cancelling and placing a new one
    #         self.send_amend_order(self.bid_id, new_bid_size)

    #     # see if we should cancel the bid
    #     elif self.bid_size > 0:

    #         # self.drift_delay is set to 0 such that we will cancel very quickly
    #         if self.etf_order_book_sequence_number >= self.bid_sequence_number + self.drift_delay and self.bid_price != new_bid_price:
    #             self.send_cancel_order(self.bid_id)

    #     # see if we should place a new bid given that we don't have any outstanding one
    #     elif new_bid_size > 0 and new_bid_price > 0:
    #         self.bid_id = next(self.order_ids)
    #         self.bid_price = new_bid_price
    #         self.bid_size = new_bid_size
    #         self.bid_original_size = self.bid_size
    #         self.bid_sequence_number = self.etf_order_book_sequence_number
    #         self.logger.info("Inserting ETF | BUY | seq number {0} | order number {1} | size {2} | price {3}".format(self.bid_sequence_number, self.bid_id, self.bid_size, self.bid_price))
    #         assert self.etf_position + self.bid_size <= POSITION_LIMIT
    #         self.send_insert_order(self.bid_id, Side.BUY, self.bid_price, self.bid_size, Lifespan.GOOD_FOR_DAY)


    #     # see if we should amend the ask order
    #     if self.ask_size > 0 and self.ask_price == new_ask_price and self.ask_size > new_ask_size: 
    #         self.send_amend_order(self.ask_id, new_ask_size)

    #     # see if we should cancel the ask order
    #     elif self.ask_size > 0:

    #         if self.etf_order_book_sequence_number >= self.ask_sequence_number + self.drift_delay and self.ask_price != new_ask_price:
    #             self.send_cancel_order(self.ask_id)

    #     elif new_ask_size > 0 and new_ask_price > 0:
    #         self.ask_id = next(self.order_ids)
    #         self.ask_price = new_ask_price
    #         self.ask_size = new_ask_size
    #         self.ask_original_size = self.ask_size
    #         self.ask_sequence_number = self.etf_order_book_sequence_number
    #         self.logger.info("Inserting ETF | SELL | seq number {0} | order number {1} | size {2} | price {3}".format(self.ask_sequence_number, self.ask_id, self.ask_size, self.ask_price))
    #         assert self.etf_position - self.ask_size >= -POSITION_LIMIT
    #         self.send_insert_order(self.ask_id, Side.SELL, self.ask_price, self.ask_size, Lifespan.GOOD_FOR_DAY)

    def update_bid(self) -> None:
        new_bid_size = self.calculate_bid_size()
        # new_bid_size = -self.etf_position + self.ask_size
        # -self.etf_position + self.ask_size < 0
        # => self.ask_size < self.etf_position
        # ???
        new_bid_price = self.calculate_bid_price()

        self.logger.info("Current bid size {0} current bid price {1} new bid size {2} new bid price {3}".format(self.bid_size, self.bid_price, new_bid_size, new_bid_price))

        # see if we should amend the bid
        if self.bid_size > 0 and self.bid_price == new_bid_price and self.bid_size > new_bid_size: 
            # TODO: If self.bid_size < new_bid_size then create a new bid order instead of cancelling and placing a new one
            self.send_amend_order(self.bid_id, new_bid_size)
        # see if we should cancel the bid
        elif self.bid_size > 0:
            if self.bid_price != new_bid_price and self.bid_sequence_number + self.cancelling_delay <= self.etf_order_book_sequence_number:
                self.logger.info("Cancelling BID with id {0}".format(self.bid_id))
                self.send_cancel_order(self.bid_id)
        # see if we should place a new bid given that we don't have any outstanding one
        elif new_bid_size > 0 and new_bid_price > 0:
            self.bid_id = next(self.order_ids)
            self.bid_price = new_bid_price
            self.bid_size = new_bid_size
            self.bid_original_size = self.bid_size
            self.bid_sequence_number = self.etf_order_book_sequence_number
            self.logger.info("Inserting ETF | BUY | seq number {0} | order number {1} | size {2} | price {3}".format(self.bid_sequence_number, self.bid_id, self.bid_size, self.bid_price))
            assert self.etf_position + self.bid_size <= POSITION_LIMIT
            self.send_insert_order(self.bid_id, Side.BUY, self.bid_price, self.bid_size, Lifespan.GOOD_FOR_DAY)

    def update_ask(self) -> None:
        new_ask_size = self.calculate_ask_size()
        # new_ask_size = (-1) * (-self.etf_position - self.bid_size)
        new_ask_price = self.calculate_ask_price()

        # see if we should amend the ask order
        if self.ask_size > 0 and self.ask_price == new_ask_price and self.ask_size > new_ask_size: 
            self.send_amend_order(self.ask_id, new_ask_size)
        # see if we should cancel the ask order
        elif self.ask_size > 0:
            if self.ask_price != new_ask_price and self.ask_sequence_number + self.cancelling_delay <= self.etf_order_book_sequence_number:
                self.send_cancel_order(self.ask_id)
        elif new_ask_size > 0 and new_ask_price > 0:
            self.ask_id = next(self.order_ids)
            self.ask_price = new_ask_price
            self.ask_size = new_ask_size
            self.ask_original_size = self.ask_size
            self.ask_sequence_number = self.etf_order_book_sequence_number
            self.logger.info("Inserting ETF | SELL | seq number {0} | order number {1} | size {2} | price {3}".format(self.ask_sequence_number, self.ask_id, self.ask_size, self.ask_price))
            assert self.etf_position - self.ask_size >= -POSITION_LIMIT
            self.send_insert_order(self.ask_id, Side.SELL, self.ask_price, self.ask_size, Lifespan.GOOD_FOR_DAY)

    def check_conditions(self) -> None:
        assert (abs(self.etf_position) <= POSITION_LIMIT and abs(self.fut_position) <= POSITION_LIMIT)
        assert (self.etf_position + self.bid_size <= POSITION_LIMIT and self.etf_position - self.ask_size >= -POSITION_LIMIT)
        assert (self.bid_size > 0 and self.bid_price > 0) or (self.bid_size == 0 and self.bid_price == 0)
        assert (self.ask_size > 0 and self.ask_price > 0) or (self.ask_size == 0 and self.ask_price == 0)
        assert (self.bid_size - self.ask_size + self.etf_position == 0)

    def calculate_bid_size(self) -> int:
        bid_size = max(0, -self.etf_position + self.ask_size)
        assert abs(self.etf_position + bid_size) <= POSITION_LIMIT
        assert bid_size >= 0
        return bid_size

    def calculate_ask_size(self) -> int:
        ask_size = max(0, (-1) * (-self.etf_position - self.bid_size))
        assert abs(self.etf_position - ask_size) <= POSITION_LIMIT
        assert ask_size >= 0
        return ask_size

    def calculate_true_price(self) -> float:
        if self.true_price_calculation == "RUNNING_AVERAGE_TRADE_PRICE" and len(self.last_etf_traded_prices) >= self.depth_long:
            price_average_short = 0.0
            price_average_long = 0.0

            total_volume = 0
            for i in range(self.depth_short):
                # it's adjusted for the volumes
                price_average_short += self.last_etf_traded_prices[i][0] # * self.last_etf_traded_prices[i][1]
                total_volume += self.last_etf_traded_prices[i][1]

            # price_average_short /= total_volume
            price_average_short /= self.depth_short

            total_volume = 0 
            for i in range(self.depth_long):
                price_average_long += self.last_etf_traded_prices[i][0] # * self.last_etf_traded_prices[i][1]
                total_volume += self.last_etf_traded_prices[i][1]

            # price_average_long /= total_volume
            price_average_long /= self.depth_long
            
            slope = 2 * (price_average_short - price_average_long) / (self.depth_long - self.depth_short)
            # true_price = (price_average_long + slope * (self.depth_long + 1) * 0.5)

            if slope > self.slope_threshold_up:
                new_true_price = self.true_price + 1

            etf_last_prices = [data[0] for data in self.last_etf_traded_prices]
            self.logger.info("Length of etf last prices {0}".format(len(etf_last_prices)))
            self.logger.info("Last ETF traded prices {0} {1} {2} {3} {4}".format(*etf_last_prices))
            self.logger.info("True Price calculated is {0}".format(true_price))

        # we assume the true price calculation is just based on the mid price
        else:
            self.true_price = (self.fut_ask_prices[0] + self.fut_bid_prices[0]) * 0.5
            self.logger.info("True Price calculated is {0}".format(self.true_price))


        return self.true_price

    # the price is given in number of tick sizes
    def calculate_bid_price(self) -> int:
        if self.fut_ask_prices[0] == 0 or self.fut_bid_prices[0] == 0:
            return 0

        true_price = self.calculate_true_price()

        one_below_true_price = math.floor((true_price - TICK_SIZE_IN_CENTS) / TICK_SIZE_IN_CENTS) * TICK_SIZE_IN_CENTS

        return one_below_true_price

    def calculate_ask_price(self) -> int:
        if self.fut_ask_prices[0] == 0 or self.fut_bid_prices[0] == 0:
            return 0

        true_price = self.calculate_true_price()

        one_above_true_price = math.ceil((true_price + TICK_SIZE_IN_CENTS) / TICK_SIZE_IN_CENTS) * TICK_SIZE_IN_CENTS

        return one_above_true_price

    def update_hedges_if_necessary(self) -> None:
        # the actual current market exposure
        current_delta = self.etf_position + self.fut_position

        if self.hedge_ask_id != 0 or self.hedge_bid_id != 0 or self.last_hedged_sequence_number + self.sequence_number_heding_delay > self.etf_order_book_sequence_number:
            return

        # TODO: Check that we haven't sent the hedge order already
        if current_delta > 0:
            self.hedge_ask_id = next(self.order_ids)
            self.hedge_ask_size = current_delta
            self.hedge_ask_price = MIN_BID_NEAREST_TICK
            self.logger.info("Inserting FUT SELL order {0} with size {1} and price {2}".format(self.hedge_ask_id, self.hedge_ask_size, self.hedge_ask_price))
            assert abs(self.fut_position - self.hedge_ask_size) <= POSITION_LIMIT
            self.send_hedge_order(self.hedge_ask_id, Side.SELL, self.hedge_ask_price, self.hedge_ask_size)

        if current_delta < 0:
            self.hedge_bid_id = next(self.order_ids)
            self.hedge_bid_size = -current_delta
            self.hedge_bid_price = MAX_ASK_NEAREST_TICK 
            self.logger.info("Inserting FUT BUY order {0} with size {1} and price {2}".format(self.hedge_bid_id, self.hedge_bid_size, self.hedge_bid_price))
            assert abs(self.fut_position + self.hedge_bid_size) <= POSITION_LIMIT
            self.send_hedge_order(self.hedge_bid_id, Side.BUY, self.hedge_bid_price, self.hedge_bid_size)
