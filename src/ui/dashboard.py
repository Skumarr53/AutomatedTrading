from flask import Flask, render_template_string
import matplotlib.pyplot as plt
import io
import base64
from src.trading_logic.trade_simulator import TradeSimulator
from src.metrics.performance_metrics import PerformanceMetrics


def create_app(simulator: TradeSimulator) -> Flask:
    app = Flask(__name__)

    @app.route('/')
    def dashboard():
        metrics = PerformanceMetrics(simulator.trade_history,
                                     simulator.starting_capital,
                                     simulator.transaction_cost)
        summary = metrics.summary()
        curve = metrics.equity_curve()
        img = ''
        if not curve.empty:
            fig, ax = plt.subplots()
            ax.plot(curve['date'], curve['balance_after_trade'])
            ax.set_title('Equity Curve')
            ax.set_xlabel('Date')
            ax.set_ylabel('Balance')
            buf = io.BytesIO()
            fig.tight_layout()
            fig.savefig(buf, format='png')
            plt.close(fig)
            buf.seek(0)
            img = base64.b64encode(buf.read()).decode('utf-8')

        html = """
        <html><head><title>Trading Dashboard</title></head><body>
        <h1>Performance Summary</h1>
        <table border='1'>
        {% for key, value in summary.items() %}
            <tr><th>{{ key }}</th><td>{{ '%.2f'|format(value) }}</td></tr>
        {% endfor %}
        </table>
        <h2>Equity Curve</h2>
        {% if img %}<img src="data:image/png;base64,{{img}}"/>{% endif %}
        </body></html>
        """
        return render_template_string(html, summary=summary, img=img)

    return app


if __name__ == '__main__':
    sim = TradeSimulator()
    app = create_app(sim)
    app.run(host='0.0.0.0', port=8050)

