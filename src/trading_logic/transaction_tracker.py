"""Utility for tracking executed transactions."""
from dataclasses import dataclass, field
from typing import List, Dict
from datetime import datetime

@dataclass
class Transaction:
    action: str
    symbol: str
    qty: int
    price: float
    timestamp: datetime

@dataclass
class TransactionTracker:
    balance: float = 0.0
    transactions: List[Transaction] = field(default_factory=list)
    positions: Dict[str, int] = field(default_factory=dict)

    def record(self, action: str, symbol: str, qty: int, price: float, timestamp: datetime) -> None:
        self.transactions.append(Transaction(action, symbol, qty, price, timestamp))
        if action == "BUY":
            self.balance -= qty * price
            self.positions[symbol] = self.positions.get(symbol, 0) + qty
        elif action == "SELL":
            self.balance += qty * price
            self.positions[symbol] = self.positions.get(symbol, 0) - qty

    def invested_amount(self, symbol: str) -> float:
        return abs(self.positions.get(symbol, 0)) * next((t.price for t in reversed(self.transactions)
                                                          if t.symbol == symbol), 0)

    def can_invest(self, symbol: str, cost: float) -> bool:
        total_value = self.balance + sum(abs(p) * next((t.price for t in reversed(self.transactions)
                                                        if t.symbol == s), 0)
                                        for s, p in self.positions.items())
        return (self.invested_amount(symbol) + cost) <= 0.1 * total_value

    def current_position(self, symbol: str) -> int:
        return self.positions.get(symbol, 0)
