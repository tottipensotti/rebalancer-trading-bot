import pandas as pd
from typing import Dict, List
from decimal import Decimal

from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import BuyOrderCompletedEvent, SellOrderCompletedEvent, MarketOrderFailureEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

class RebalanceBot(ScriptStrategyBase):
    connector_name: str = "binance_paper_trade"
    portfolio_allocation: Dict = {
        "USDT": 0.5,
        "BTC": 0.2,
        "ETH": 0.3,
    }

    min_order_amount_to_rebalance_quote: Decimal = Decimal("10")
    processed_data: Dict = {
        "actual_portfolio":{},
        "theorical_portfolio": {},
        "diff_actual_vs_theorical": {}
    }

    trading_pairs = {f"{token}-USDT" for token in portfolio_allocation if token != "USDT"}
    markets = {connector_name: trading_pairs}

    @property
    def connector(self):
        return self.connectors[self.connector_name]
    
    @property
    def get_allocation(self):
        total_sum = sum(self.portfolio_allocation.values())
        portfolio_allocation_normalized = {}
        
        for token, value in self.portfolio_allocation.items():
            portfolio_allocation_normalized[token] = Decimal(value / total_sum)
        
        return portfolio_allocation_normalized

    def on_tick(self):
        self.compute_portfolio_metrics()
        proposal: List[OrderCandidate] = self.get_rebalance_proposal()
        proposal_adjusted: List[OrderCandidate] = self.adjust_proposal_to_budget(proposal)
        self.place_proposal(proposal_adjusted)
    
    def compute_portfolio_metrics(self):
        self.compute_portfolio_value()
        self.compute_theoretical_portfolio()
        self.compute_portfolio_diff()
    
    def get_token_price(self, token):
        if token == "USDT":
            return Decimal("1")
        else:
            return self.connector.get_mid_price(f"{token}-USDT")

    
    def compute_portfolio_value(self):
        for token in self.get_allocation().keys():
            balance = self.connector.get_balance(token)
            price = self.connector.get_token_price(token)
            self.processed_data["actual_portfolio"][token] = balance * price

    def compute_theoretical_portfolio(self):
        total_portfolio = sum(self.processed_data["actual_portfolio"].values())
        for token, allocation in self.get_allocation().items():
            self.processed_data["theorical_portfolio"][token] = total_portfolio * allocation

    def compute_portfolio_diff(self):
        for token in self.get_allocation().keys():
            self.processed_data["diff"][token] = self.processed_data["theorical_portfolio"][token] - self.processed_data["actual_portfolio"][token]
        
    def get_rebalance_proposal(self):
        proposal = []

        if len(self.active_rebalance_orders) == 0:
            for token, diff in self.processed_data["diff"].items():
                if diff < self.min_order_amount_to_rebalance_quote or token == "USDT":
                    continue
        
                price = self.get_token_price(token)
                amount = diff / price
                order_side = TradeType.BUY if diff>0 else TradeType.SELL
                order_candidate = OrderCandidate(trading_pair=f"{token}-USDT",
                                                is_maker=False,
                                                order_type=OrderType.MARKET,
                                                order_side=order_side,
                                                amount=amount,
                                                price=price)
                proposal.append(order_candidate)
                
                return proposal
    
    def adjust_proposal_to_budget(self, proposal):
        return self.connector.budget_checker.adjust_candidates(proposal, True)
    
    def place_proposal(self, proposal: List[OrderCandidate]) -> None:
        for order in proposal:
            if order.amount >= Decimal("0"):
                self.place_order(connector_name=self.connector_name, order=order)

    def place_order(self, connector_name: str, order: OrderCandidate):
        oid = None
        
        if order.order_side == TradeType.SELL:
            oid = self.sell(connector_name=connector_name, trading_pair=order.trading_pair, amount=order.amount,
                                order_type=order.order_type, price=order.price)
        
        elif order.order_side == TradeType.BUY:
            oid = self.buy(connector_name=connector_name, trading_pair=order.trading_pair, amount=order.amount,
                                order_type=order.order_type, price=order.price)
        
        if oid:
            self.active_rebalance_orders.append(oid)
    
    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        
        lines = []
        df = pd.DataFrame(self.processed_data).round(2)
        lines.extend([format_df_for_printout(df, table_format='psql')])
        
        return "\n".join(lines)
    
    def did_complete_buy_order(self, order_completed_event: BuyOrderCompletedEvent):
        self.review_rebalance_orders(order_completed_event.order_id)
    def did_complete_sell_order(self, order_completed_event: SellOrderCompletedEvent):
        self.review_rebalance_orders(order_completed_event.order_id)

    def did_fail_order(self, order_failed_event: MarketOrderFailureEvent):
        self.review_balance_orders(order_failed_event)

    def review_rebalance_orders(self, order_id):
        self.active_rebalance_orders = [order for order in self.active_rebalance_orders if order != order_id]
