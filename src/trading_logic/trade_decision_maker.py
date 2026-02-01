# src/trading_logic/trade_decision_maker.py
"""
Enhanced trade decision maker with ATR-based volatility confidence,
probability weighting, and timeframe agreement scoring.
"""
from datetime import datetime
from typing import Dict, Optional, Tuple, Any
from dataclasses import dataclass
from loguru import logger


@dataclass
class TradeDecision:
    """
    Structured trade decision output.
    
    Attributes:
        direction: 'LONG', 'SHORT', or 'HOLD'
        confidence: Overall confidence score (0.0 to 1.0)
        weighted_score: Raw weighted signal score
        timeframe_agreement: Number of timeframes agreeing on direction
        volatility_assessment: 'LOW', 'MEDIUM', or 'HIGH'
        position_size_multiplier: Suggested position sizing factor (0.5 to 1.5)
        stop_loss_atr_multiplier: Suggested ATR multiplier for stop loss
        reason: Human-readable explanation
    """
    direction: str
    confidence: float
    weighted_score: float
    timeframe_agreement: int
    volatility_assessment: str
    position_size_multiplier: float
    stop_loss_atr_multiplier: float
    reason: str


class TradeDecisionMaker:
    """
    Enhanced trade decision maker with:
    - ATR-based volatility confidence
    - Prediction probability weighting  
    - Timeframe agreement scoring
    - Configurable parameters
    
    Signal Flow:
    ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
    │ PctChange   │   │ ATR         │   │ Probability │
    │ Predictions │   │ Predictions │   │ Weights     │
    └─────┬───────┘   └─────┬───────┘   └─────┬───────┘
          │                 │                 │
          ▼                 ▼                 ▼
    ┌─────────────────────────────────────────────────┐
    │           Signal Processing Engine              │
    ├─────────────────────────────────────────────────┤
    │  1. Weighted Score (by timeframe)               │
    │  2. Volatility Assessment (from ATR)            │
    │  3. Confidence Calculation (from probas)        │
    │  4. Timeframe Agreement Check                   │
    └─────────────────────────────────────────────────┘
                          │
                          ▼
    ┌─────────────────────────────────────────────────┐
    │              TradeDecision Output               │
    │  direction, confidence, position_size, etc.     │
    └─────────────────────────────────────────────────┘
    """

    # Default configuration (can be overridden by config file)
    DEFAULT_CONFIG = {
        'timeframe_weights': {
            '5min': 1.0, '5m': 1.0,
            '15min': 2.0, '15m': 2.0,
            '30min': 3.0, '30m': 3.0,
            '1h': 4.0,
            '3h': 5.0,
        },
        'signal_mapping': {
            'Low': -2, 'Medium Low': -1, 'Neutral': 0, 'Medium High': 1, 'High': 2
        },
        'atr_volatility_thresholds': {
            'low': 0.02,   # Below this = low volatility
            'high': 0.05,  # Above this = high volatility
        },
        'min_confidence': 0.5,  # Minimum confidence to trade
        'min_timeframe_agreement': 2,  # Minimum timeframes agreeing for high confidence
        'buy_score_threshold': 0.5,  # Score above this = LONG
        'sell_score_threshold': -0.5,  # Score below this = SHORT
        'cooldown_minutes': 5,
    }

    def __init__(
        self, 
        config: Optional[Dict[str, Any]] = None,
        cooldown_minutes: Optional[int] = None,
    ):
        """
        Initialize enhanced decision maker.
        
        Args:
            config: Configuration dictionary (merged with defaults)
            cooldown_minutes: Override for cooldown period
        """
        # Merge provided config with defaults
        self._config = {**self.DEFAULT_CONFIG}
        if config:
            self._config.update(config)
        
        # Allow direct cooldown override
        if cooldown_minutes is not None:
            self._config['cooldown_minutes'] = cooldown_minutes
        
        self.cooldown_minutes = self._config['cooldown_minutes']
        self.last_trade_time: Optional[datetime] = None
        self.pending_signal: Optional[str] = None
        self.pending_signal_time: Optional[datetime] = None
        
        logger.info(f"TradeDecisionMaker initialized with config: min_confidence={self._config['min_confidence']}, "
                   f"min_agreement={self._config['min_timeframe_agreement']}")

    @property
    def weights(self) -> Dict[str, float]:
        """Timeframe weights for backward compatibility."""
        return self._config['timeframe_weights']

    def convert_signal_to_score(self, signal: str) -> float:
        """
        Convert model prediction category to numeric score.
        
        Maps: Low=-2, Medium Low=-1, Neutral=0, Medium High=1, High=2
        """
        mapping = self._config['signal_mapping']
        return mapping.get(signal, 0.0)

    def compute_weighted_signal(
        self, 
        pct_predictions: Dict[str, str],
        probabilities: Optional[Dict[str, float]] = None,
    ) -> Tuple[float, Dict[str, float]]:
        """
        Compute weighted signal score from PctChange predictions.
        
        Args:
            pct_predictions: Dict of {timeframe: prediction_label}
            probabilities: Optional dict of {timeframe: max_probability}
            
        Returns:
            Tuple of (weighted_score, per_timeframe_scores)
        """
        weights = self._config['timeframe_weights']
        total_weight = 0.0
        weighted_sum = 0.0
        per_tf_scores = {}
        
        for tf, signal in pct_predictions.items():
            score = self.convert_signal_to_score(signal)
            
            # Base weight from config
            w = weights.get(tf, 1.0)
            
            # Optional: boost weight by prediction probability
            if probabilities and tf in probabilities:
                prob = probabilities[tf]
                # Scale weight by confidence: low prob (0.5) = no boost, high prob (1.0) = 1.5x boost
                prob_multiplier = 0.5 + prob
                w *= prob_multiplier
            
            weighted_sum += score * w
            total_weight += w
            per_tf_scores[tf] = score
        
        normalized_score = weighted_sum / total_weight if total_weight > 0 else 0.0
        return normalized_score, per_tf_scores

    def compute_timeframe_agreement(self, per_tf_scores: Dict[str, float]) -> Tuple[int, int, int]:
        """
        Count how many timeframes agree on direction.
        
        Returns:
            Tuple of (bullish_count, bearish_count, neutral_count)
        """
        bullish = sum(1 for s in per_tf_scores.values() if s > 0)
        bearish = sum(1 for s in per_tf_scores.values() if s < 0)
        neutral = sum(1 for s in per_tf_scores.values() if s == 0)
        
        return bullish, bearish, neutral

    def assess_volatility(self, atr_predictions: Optional[Dict[str, str]] = None) -> Tuple[str, float]:
        """
        Assess market volatility from ATR predictions.
        
        Returns:
            Tuple of (volatility_label, confidence_adjustment)
            
        Volatility Impact on Trading:
        - LOW volatility + strong signal = HIGH confidence (stable trend)
        - HIGH volatility + strong signal = MEDIUM confidence (risky)
        - HIGH volatility + weak signal = LOW confidence (avoid)
        """
        if not atr_predictions:
            return 'MEDIUM', 1.0  # Default: no adjustment
        
        thresholds = self._config['atr_volatility_thresholds']
        
        # Count ATR prediction categories
        high_atr_count = sum(1 for p in atr_predictions.values() if p in ['High', 'Medium High'])
        low_atr_count = sum(1 for p in atr_predictions.values() if p in ['Low', 'Medium Low'])
        total = len(atr_predictions)
        
        if total == 0:
            return 'MEDIUM', 1.0
        
        # Determine volatility regime
        high_ratio = high_atr_count / total
        low_ratio = low_atr_count / total
        
        if high_ratio >= 0.6:
            # High volatility: reduce confidence, widen stops
            return 'HIGH', 0.7
        elif low_ratio >= 0.6:
            # Low volatility: boost confidence for strong signals
            return 'LOW', 1.2
        else:
            return 'MEDIUM', 1.0

    def compute_position_sizing(
        self, 
        confidence: float, 
        volatility: str, 
        timeframe_agreement: int,
    ) -> Tuple[float, float]:
        """
        Compute position size multiplier and stop loss ATR multiplier.
        
        Returns:
            Tuple of (position_size_multiplier, stop_loss_atr_multiplier)
        """
        # Base position size multiplier
        pos_mult = confidence  # Higher confidence = larger position
        
        # Adjust for volatility
        if volatility == 'HIGH':
            pos_mult *= 0.7  # Reduce size in high volatility
            sl_mult = 2.5    # Wider stop loss
        elif volatility == 'LOW':
            pos_mult *= 1.1  # Slight increase in stable markets
            sl_mult = 1.5    # Tighter stop loss
        else:
            sl_mult = 2.0    # Default
        
        # Adjust for timeframe agreement
        min_agreement = self._config['min_timeframe_agreement']
        if timeframe_agreement >= min_agreement + 2:
            pos_mult *= 1.2  # Strong agreement boost
        elif timeframe_agreement < min_agreement:
            pos_mult *= 0.7  # Weak agreement reduction
        
        # Clamp to reasonable bounds
        pos_mult = max(0.3, min(1.5, pos_mult))
        sl_mult = max(1.0, min(4.0, sl_mult))
        
        return pos_mult, sl_mult

    def make_decision(
        self,
        pct_predictions: Dict[str, str],
        atr_predictions: Optional[Dict[str, str]] = None,
        probabilities: Optional[Dict[str, float]] = None,
    ) -> TradeDecision:
        """
        Make enhanced trading decision combining all signals.
        
        Args:
            pct_predictions: PctChange predictions per timeframe
            atr_predictions: ATR predictions per timeframe (optional)
            probabilities: Max prediction probabilities per timeframe (optional)
            
        Returns:
            TradeDecision with direction, confidence, sizing, etc.
        """
        # 1. Compute weighted PctChange score
        weighted_score, per_tf_scores = self.compute_weighted_signal(pct_predictions, probabilities)
        
        # 2. Count timeframe agreement
        bullish, bearish, neutral = self.compute_timeframe_agreement(per_tf_scores)
        agreement_count = max(bullish, bearish)
        
        # 3. Assess volatility from ATR
        volatility, vol_adjustment = self.assess_volatility(atr_predictions)
        
        # 4. Calculate base confidence
        # Confidence is based on: score magnitude, timeframe agreement, volatility
        score_magnitude = abs(weighted_score) / 2.0  # Normalize to 0-1 range
        agreement_ratio = agreement_count / max(len(pct_predictions), 1)
        
        base_confidence = (score_magnitude * 0.4 + agreement_ratio * 0.4 + 0.2)
        confidence = min(1.0, base_confidence * vol_adjustment)
        
        # 5. Determine direction
        buy_threshold = self._config['buy_score_threshold']
        sell_threshold = self._config['sell_score_threshold']
        min_confidence = self._config['min_confidence']
        
        if weighted_score > buy_threshold and confidence >= min_confidence:
            direction = 'LONG'
            reason = f"Bullish: score={weighted_score:.2f}, {bullish}/{len(pct_predictions)} TFs agree"
        elif weighted_score < sell_threshold and confidence >= min_confidence:
            direction = 'SHORT'
            reason = f"Bearish: score={weighted_score:.2f}, {bearish}/{len(pct_predictions)} TFs agree"
        else:
            direction = 'HOLD'
            if confidence < min_confidence:
                reason = f"Low confidence ({confidence:.2f} < {min_confidence})"
            else:
                reason = f"Neutral zone: score={weighted_score:.2f}"
        
        # 6. Compute position sizing
        pos_mult, sl_mult = self.compute_position_sizing(confidence, volatility, agreement_count)
        
        return TradeDecision(
            direction=direction,
            confidence=confidence,
            weighted_score=weighted_score,
            timeframe_agreement=agreement_count,
            volatility_assessment=volatility,
            position_size_multiplier=pos_mult,
            stop_loss_atr_multiplier=sl_mult,
            reason=reason,
        )

    def is_cooldown_over(self, current_time: datetime) -> bool:
        """Check if the cooldown period after the last trade has passed."""
        if self.last_trade_time is None:
            return True
        elapsed_minutes = (current_time - self.last_trade_time).total_seconds() / 60.0
        return elapsed_minutes >= self.cooldown_minutes

    def confirm_signal(self, signal: str, current_time: datetime) -> bool:
        """
        Confirm signal by requiring it to persist across multiple checks.
        """
        if signal is None:
            self.pending_signal = None
            return False
        if self.pending_signal is None or signal != self.pending_signal:
            self.pending_signal = signal
            self.pending_signal_time = current_time
            return False
        return True

    def record_trade(self, current_time: datetime) -> None:
        """Record that a trade was executed for cooldown tracking."""
        self.last_trade_time = current_time
        self.pending_signal = None
